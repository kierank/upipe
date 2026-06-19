/*
 * Copyright (C) 2026 Open Broadcast Systems Ltd
 *
 * Authors: Kieran Kunhya
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 */

/** @file
 * @short Upipe libzvbi CEA-608 closed caption decoding module
 *
 * This is essentially the inverse of @ref upipe_zvbienc: instead of burning
 * captions into VBI lines, it reads the CEA-708 cc_data triplets carried as
 * the @ref uref_pic_get_cea_708 opaque attribute on incoming picture urefs,
 * feeds the CEA-608 (line 21) captions they contain into a libzvbi decoder,
 * and renders the selected caption channel to an alpha-keyed RGBA
 * sub-picture suitable for overlay (e.g. @ref upipe_blit /
 * @ref upipe_subpic_schedule), much like the ffmpeg teletext/dvbsub decoder
 * in @ref upipe_avcdec.
 *
 * The input picture urefs are consumed (not forwarded); only sub-pictures are
 * emitted. The video itself is expected to reach the overlay separately.
 */

#include "upipe/ubase.h"
#include "upipe/uclock.h"
#include "upipe/uref.h"
#include "upipe/uref_flow.h"
#include "upipe/uref_clock.h"
#include "upipe/uref_pic.h"
#include "upipe/uref_pic_flow.h"
#include "upipe/uref_pic_flow_formats.h"
#include "upipe/ubuf_pic.h"
#include "upipe/upipe.h"
#include "upipe/upipe_helper_upipe.h"
#include "upipe/upipe_helper_urefcount.h"
#include "upipe/upipe_helper_void.h"
#include "upipe/upipe_helper_output.h"
#include "upipe/upipe_helper_ubuf_mgr.h"
#include "upipe-zvbi/upipe_zvbidec.h"

#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>

#include <libzvbi.h>

/** width in pixels of a libzvbi caption character cell */
#define CCW 16
/** height in pixels of a libzvbi caption character cell */
#define CCH 26

/** @internal @This is the default amount of time a rendered caption is
 * displayed, when nothing has replaced it yet (in @ref UCLOCK_FREQ units).
 * Closed captions persist on screen until updated or erased; the decoder
 * emits a fresh (possibly transparent) sub-picture whenever the displayed
 * page changes, and this caps how long a stale caption lingers. */
#define DEFAULT_DURATION (UCLOCK_FREQ * 16)

/** the chroma of the rendered sub-picture (matches libzvbi VBI_PIXFMT_RGBA32_LE,
 * which stores bytes in R, G, B, A order) */
#define ZVBIDEC_CHROMA "r8g8b8a8"

/** upipe_zvbidec structure */
struct upipe_zvbidec {
    /** refcount management structure */
    struct urefcount urefcount;

    /** output pipe */
    struct upipe *output;
    /** flow definition packet */
    struct uref *flow_def;
    /** output state */
    enum upipe_helper_output_state output_state;
    /** list of output requests */
    struct uchain request_list;

    /** ubuf manager for the rendered sub-pictures */
    struct ubuf_mgr *ubuf_mgr;
    /** flow format for the ubuf manager request */
    struct uref *flow_format;
    /** ubuf manager request */
    struct urequest ubuf_mgr_request;

    /** input flow definition (the video carrying the captions) */
    struct uref *flow_def_input;
    /** the output flow definition needs to be rebuilt */
    bool flow_def_sent;

    /** libzvbi decoder */
    vbi_decoder *vbi;
    /** selected caption channel (0-7), maps to libzvbi page number channel+1 */
    int channel;

    /** horizontal size of the video (for positioning), or UINT64_MAX */
    uint64_t hsize;
    /** vertical size of the video (for positioning), or UINT64_MAX */
    uint64_t vsize;
    /** geometry of the last emitted sub-picture */
    uint64_t out_hsize, out_vsize, out_hposition, out_vposition;

    /** monotonic timestamp handed to libzvbi */
    double timestamp;

    /** uref currently being processed (borrowed, used to stamp output) */
    struct uref *uref;
    /** set by the libzvbi event handler when the selected page changed */
    bool page_dirty;

    /** public upipe structure */
    struct upipe upipe;
};

UPIPE_HELPER_UPIPE(upipe_zvbidec, upipe, UPIPE_ZVBIDEC_SIGNATURE);
UPIPE_HELPER_UREFCOUNT(upipe_zvbidec, urefcount, upipe_zvbidec_free)
UPIPE_HELPER_VOID(upipe_zvbidec);
UPIPE_HELPER_OUTPUT(upipe_zvbidec, output, flow_def, output_state, request_list)
UPIPE_HELPER_UBUF_MGR(upipe_zvbidec, ubuf_mgr, flow_format, ubuf_mgr_request,
                      NULL,
                      upipe_zvbidec_register_output_request,
                      upipe_zvbidec_unregister_output_request)

/** @internal @This is called by libzvbi when a decoding event occurs.
 *
 * @param event libzvbi event
 * @param user_data pointer to the upipe structure
 */
static void upipe_zvbidec_event(vbi_event *event, void *user_data)
{
    struct upipe *upipe = user_data;
    struct upipe_zvbidec *upipe_zvbidec = upipe_zvbidec_from_upipe(upipe);

    if (event->type != VBI_EVENT_CAPTION)
        return;

    /* libzvbi caption page numbers are 1-based: 1-4 = CC1-CC4,
     * 5-8 = TEXT1-TEXT4. */
    if (event->ev.caption.pgno == upipe_zvbidec->channel + 1)
        upipe_zvbidec->page_dirty = true;
}

/** @internal @This (re)builds and stores the output flow definition for the
 * given sub-picture geometry.
 *
 * @param upipe description structure of the pipe
 * @param hsize horizontal size of the rendered bitmap
 * @param vsize vertical size of the rendered bitmap
 * @param hposition horizontal position of the bitmap in the video
 * @param vposition vertical position of the bitmap in the video
 * @return an error code
 */
static int upipe_zvbidec_build_flow_def(struct upipe *upipe,
        uint64_t hsize, uint64_t vsize,
        uint64_t hposition, uint64_t vposition)
{
    struct upipe_zvbidec *upipe_zvbidec = upipe_zvbidec_from_upipe(upipe);

    if (upipe_zvbidec->flow_def_sent &&
        hsize == upipe_zvbidec->out_hsize &&
        vsize == upipe_zvbidec->out_vsize &&
        hposition == upipe_zvbidec->out_hposition &&
        vposition == upipe_zvbidec->out_vposition)
        return UBASE_ERR_NONE;

    if (unlikely(upipe_zvbidec->flow_def_input == NULL))
        return UBASE_ERR_INVALID;

    struct uref *flow_def = uref_dup(upipe_zvbidec->flow_def_input);
    if (unlikely(flow_def == NULL))
        return UBASE_ERR_ALLOC;

    struct urational sar = { .num = 1, .den = 1 };
    if (unlikely(
            !ubase_check(uref_flow_set_def(flow_def, UREF_PIC_SUB_FLOW_DEF)) ||
            !ubase_check(uref_pic_flow_set_rgba(flow_def)) ||
            !ubase_check(uref_pic_set_progressive(flow_def, true)) ||
            !ubase_check(uref_pic_flow_set_full_range(flow_def)) ||
            !ubase_check(uref_pic_flow_set_sar(flow_def, sar)) ||
            !ubase_check(uref_pic_flow_set_hsize(flow_def, hsize)) ||
            !ubase_check(uref_pic_flow_set_vsize(flow_def, vsize)) ||
            !ubase_check(uref_pic_flow_set_hsize_visible(flow_def, hsize)) ||
            !ubase_check(uref_pic_flow_set_vsize_visible(flow_def, vsize)) ||
            !ubase_check(uref_pic_set_hposition(flow_def, hposition)) ||
            !ubase_check(uref_pic_set_vposition(flow_def, vposition)))) {
        uref_free(flow_def);
        return UBASE_ERR_INVALID;
    }

    upipe_zvbidec->out_hsize = hsize;
    upipe_zvbidec->out_vsize = vsize;
    upipe_zvbidec->out_hposition = hposition;
    upipe_zvbidec->out_vposition = vposition;
    upipe_zvbidec->flow_def_sent = true;

    upipe_zvbidec_store_flow_def(upipe, flow_def);
    return UBASE_ERR_NONE;
}

/** @internal @This renders the currently displayed caption page to a
 * sub-picture and outputs it.
 *
 * @param upipe description structure of the pipe
 * @param upump_p reference to pump that generated the buffer
 */
static void upipe_zvbidec_render(struct upipe *upipe, struct upump **upump_p)
{
    struct upipe_zvbidec *upipe_zvbidec = upipe_zvbidec_from_upipe(upipe);

    vbi_page pg;
    if (!vbi_fetch_cc_page(upipe_zvbidec->vbi, &pg,
                           upipe_zvbidec->channel + 1, FALSE))
        return;
    if (unlikely(pg.columns <= 0 || pg.rows <= 0))
        return;

    uint64_t hsize = (uint64_t)pg.columns * CCW;
    uint64_t vsize = (uint64_t)pg.rows * CCH;

    /* Center the caption grid in the video; the application can reposition or
     * scale it downstream (e.g. via upipe_blit margins/rect). */
    uint64_t hposition = 0, vposition = 0;
    if (upipe_zvbidec->hsize != UINT64_MAX && upipe_zvbidec->hsize > hsize)
        hposition = (upipe_zvbidec->hsize - hsize) / 2;
    if (upipe_zvbidec->vsize != UINT64_MAX && upipe_zvbidec->vsize > vsize)
        vposition = (upipe_zvbidec->vsize - vsize) / 2;

    /* Make sure we have a ubuf manager for the sub-pictures. */
    if (unlikely(upipe_zvbidec->ubuf_mgr == NULL)) {
        if (unlikely(upipe_zvbidec->flow_def_input == NULL))
            return;
        struct uref *flow_format = uref_dup(upipe_zvbidec->flow_def_input);
        if (unlikely(flow_format == NULL)) {
            upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
            return;
        }
        if (unlikely(
                !ubase_check(uref_flow_set_def(flow_format,
                        UREF_PIC_SUB_FLOW_DEF)) ||
                !ubase_check(uref_pic_flow_set_rgba(flow_format)) ||
                !ubase_check(uref_pic_set_progressive(flow_format, true)) ||
                !ubase_check(uref_pic_flow_set_full_range(flow_format)))) {
            uref_free(flow_format);
            upipe_throw_fatal(upipe, UBASE_ERR_INVALID);
            return;
        }
        if (unlikely(!upipe_zvbidec_demand_ubuf_mgr(upipe, flow_format)))
            return;
    }

    if (unlikely(!ubase_check(upipe_zvbidec_build_flow_def(upipe, hsize, vsize,
                        hposition, vposition))))
        return;

    struct ubuf *ubuf = ubuf_pic_alloc(upipe_zvbidec->ubuf_mgr, hsize, vsize);
    if (unlikely(ubuf == NULL)) {
        upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
        return;
    }

    uint8_t *buf;
    size_t stride;
    if (unlikely(
            !ubase_check(ubuf_pic_plane_size(ubuf, ZVBIDEC_CHROMA, &stride,
                    NULL, NULL, NULL)) ||
            !ubase_check(ubuf_pic_plane_write(ubuf, ZVBIDEC_CHROMA,
                    0, 0, -1, -1, &buf)))) {
        ubuf_free(ubuf);
        upipe_throw_fatal(upipe, UBASE_ERR_INVALID);
        return;
    }

    /* libzvbi writes every pixel of the page region (transparent cells get a
     * zero alpha), so the whole visible bitmap is covered. */
    vbi_draw_cc_page_region(&pg, VBI_PIXFMT_RGBA32_LE, buf, (int)stride,
                            0, 0, pg.columns, pg.rows);

    ubuf_pic_plane_unmap(ubuf, ZVBIDEC_CHROMA, 0, 0, -1, -1);

    /* Duplicate the source uref to inherit its clock (the time the caption
     * became visible) and attach the rendered bitmap. */
    struct uref *uref = uref_dup(upipe_zvbidec->uref);
    if (unlikely(uref == NULL)) {
        ubuf_free(ubuf);
        upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
        return;
    }
    uref_attach_ubuf(uref, ubuf);
    uref_pic_set_progressive(uref, true);

    /* The inherited duration is the video frame's; a caption persists until it
     * is updated or erased, so cap how long a stale one lingers instead. */
    uref_clock_set_duration(uref, DEFAULT_DURATION);

    upipe_zvbidec_output(upipe, uref, upump_p);
}

/** @internal @This receives incoming uref.
 *
 * @param upipe description structure of the pipe
 * @param uref uref structure describing a picture carrying captions
 * @param upump_p reference to pump that generated the buffer
 */
static void upipe_zvbidec_input(struct upipe *upipe, struct uref *uref,
                                struct upump **upump_p)
{
    struct upipe_zvbidec *upipe_zvbidec = upipe_zvbidec_from_upipe(upipe);

    const uint8_t *pic_data = NULL;
    size_t pic_data_size = 0;
    uref_pic_get_cea_708(uref, &pic_data, &pic_data_size);

    if (pic_data_size >= 3) {
        /* Build sliced VBI from the CEA-608 (line 21) cc pairs. cc_type 0 and 1
         * are field 1 and field 2; cc_type 2 and 3 are CEA-708 DTVCC, which
         * libzvbi does not decode. */
        vbi_sliced sliced[2];
        for (int i = 0; i < 2; i++) {
            memset(sliced[i].data, 0, sizeof(sliced[i].data));
            sliced[i].id = VBI_SLICED_NONE;
        }
        sliced[0].id = VBI_SLICED_CAPTION_525_F1;
        sliced[0].line = 21;
        sliced[1].id = VBI_SLICED_CAPTION_525_F2;
        sliced[1].line = 284;
        bool present[2] = { false, false };

        for (size_t i = 0; i < pic_data_size / 3; i++) {
            const uint8_t valid = (pic_data[3 * i] >> 2) & 1;
            const uint8_t cc_type = pic_data[3 * i] & 0x3;

            if (valid && cc_type < 2) {
                memcpy(sliced[cc_type].data, &pic_data[3 * i + 1], 2);
                present[cc_type] = true;
            }
        }

        /* Pack the present fields contiguously for libzvbi. */
        vbi_sliced decode[2];
        int lines = 0;
        for (int i = 0; i < 2; i++)
            if (present[i])
                decode[lines++] = sliced[i];

        if (lines) {
            /* libzvbi wants a monotonically increasing timestamp in seconds. */
            double ts;
            uint64_t date;
            int type;
            uref_clock_get_date_prog(uref, &date, &type);
            if (date != UINT64_MAX)
                ts = (double)date / UCLOCK_FREQ;
            else
                ts = upipe_zvbidec->timestamp + 1. / 30;
            if (ts <= upipe_zvbidec->timestamp)
                ts = upipe_zvbidec->timestamp + 1. / 30;
            upipe_zvbidec->timestamp = ts;

            upipe_zvbidec->uref = uref;
            upipe_zvbidec->page_dirty = false;
            vbi_decode(upipe_zvbidec->vbi, decode, lines, ts);
            if (upipe_zvbidec->page_dirty)
                upipe_zvbidec_render(upipe, upump_p);
            upipe_zvbidec->uref = NULL;
        }
    }

    /* The input video is not forwarded; only sub-pictures are emitted. */
    uref_free(uref);
}

/** @internal @This sets the input flow definition.
 *
 * @param upipe description structure of the pipe
 * @param flow_def flow definition packet
 * @return an error code
 */
static int upipe_zvbidec_set_flow_def(struct upipe *upipe, struct uref *flow_def)
{
    struct upipe_zvbidec *upipe_zvbidec = upipe_zvbidec_from_upipe(upipe);

    if (flow_def == NULL)
        return UBASE_ERR_INVALID;
    UBASE_RETURN(uref_flow_match_def(flow_def, UREF_PIC_FLOW_DEF));

    uint64_t hsize = UINT64_MAX, vsize = UINT64_MAX;
    uref_pic_flow_get_hsize(flow_def, &hsize);
    uref_pic_flow_get_vsize(flow_def, &vsize);

    struct uref *flow_def_dup = uref_dup(flow_def);
    if (unlikely(flow_def_dup == NULL))
        return UBASE_ERR_ALLOC;

    uref_free(upipe_zvbidec->flow_def_input);
    upipe_zvbidec->flow_def_input = flow_def_dup;
    upipe_zvbidec->hsize = hsize;
    upipe_zvbidec->vsize = vsize;
    /* force the output flow def to be rebuilt (position depends on the
     * video size) */
    upipe_zvbidec->flow_def_sent = false;

    return UBASE_ERR_NONE;
}

/** @internal @This sets the caption channel to render.
 *
 * @param upipe description structure of the pipe
 * @param channel caption channel (0-7)
 * @return an error code
 */
static int _upipe_zvbidec_set_channel(struct upipe *upipe, int channel)
{
    struct upipe_zvbidec *upipe_zvbidec = upipe_zvbidec_from_upipe(upipe);
    if (channel < UPIPE_ZVBIDEC_CC1 || channel > UPIPE_ZVBIDEC_TEXT4)
        return UBASE_ERR_INVALID;
    upipe_zvbidec->channel = channel;
    return UBASE_ERR_NONE;
}

/** @internal @This gets the caption channel being rendered.
 *
 * @param upipe description structure of the pipe
 * @param channel_p filled in with the caption channel
 * @return an error code
 */
static int _upipe_zvbidec_get_channel(struct upipe *upipe, int *channel_p)
{
    struct upipe_zvbidec *upipe_zvbidec = upipe_zvbidec_from_upipe(upipe);
    if (channel_p != NULL)
        *channel_p = upipe_zvbidec->channel;
    return UBASE_ERR_NONE;
}

/** @internal @This processes control commands on a zvbidec pipe.
 *
 * @param upipe description structure of the pipe
 * @param command type of command to process
 * @param args arguments of the command
 * @return an error code
 */
static int upipe_zvbidec_control(struct upipe *upipe, int command, va_list args)
{
    UBASE_HANDLED_RETURN(upipe_zvbidec_control_ubuf_mgr(upipe, command, args));
    UBASE_HANDLED_RETURN(upipe_zvbidec_control_output(upipe, command, args));
    switch (command) {
        case UPIPE_SET_FLOW_DEF: {
            struct uref *flow_def = va_arg(args, struct uref *);
            return upipe_zvbidec_set_flow_def(upipe, flow_def);
        }
        case UPIPE_ZVBIDEC_SET_CHANNEL: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_ZVBIDEC_SIGNATURE);
            int channel = va_arg(args, int);
            return _upipe_zvbidec_set_channel(upipe, channel);
        }
        case UPIPE_ZVBIDEC_GET_CHANNEL: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_ZVBIDEC_SIGNATURE);
            int *channel_p = va_arg(args, int *);
            return _upipe_zvbidec_get_channel(upipe, channel_p);
        }
        default:
            return UBASE_ERR_UNHANDLED;
    }
}

/** @internal @This is the libzvbi logging callback.
 *
 * @param level libzvbi log level
 * @param context libzvbi context string
 * @param message log message
 * @param user_data pointer to the upipe structure
 */
static void upipe_zvbidec_log(vbi_log_mask level, const char *context,
                              const char *message, void *user_data)
{
    struct upipe *upipe = user_data;

    enum uprobe_log_level l = UPROBE_LOG_INFO;
    if (level & VBI_LOG_ERROR)
        l = UPROBE_LOG_ERROR;
    else if (level & VBI_LOG_WARNING)
        l = UPROBE_LOG_WARNING;
    else if (level & VBI_LOG_NOTICE)
        l = UPROBE_LOG_NOTICE;
    else if (level & VBI_LOG_INFO)
        l = UPROBE_LOG_INFO;
    else if (level & VBI_LOG_DEBUG)
        l = UPROBE_LOG_DEBUG;

    upipe_log_va(upipe, l, "%s: %s", context, message);
}

/** @internal @This allocates a zvbidec pipe.
 *
 * @param mgr common management structure
 * @param uprobe structure used to raise events
 * @param signature signature of the pipe allocator
 * @param args optional arguments
 * @return pointer to upipe or NULL in case of allocation error
 */
static struct upipe *upipe_zvbidec_alloc(struct upipe_mgr *mgr,
                                         struct uprobe *uprobe,
                                         uint32_t signature, va_list args)
{
    struct upipe *upipe = upipe_zvbidec_alloc_void(mgr, uprobe, signature, args);
    if (unlikely(upipe == NULL))
        return NULL;

    struct upipe_zvbidec *upipe_zvbidec = upipe_zvbidec_from_upipe(upipe);

    vbi_set_log_fn(VBI_LOG_NOTICE | VBI_LOG_WARNING | VBI_LOG_ERROR |
                   VBI_LOG_INFO, upipe_zvbidec_log, upipe);

    upipe_zvbidec->vbi = vbi_decoder_new();
    if (unlikely(upipe_zvbidec->vbi == NULL)) {
        upipe_zvbidec_free_void(upipe);
        return NULL;
    }
    vbi_event_handler_add(upipe_zvbidec->vbi, VBI_EVENT_CAPTION,
                          upipe_zvbidec_event, upipe);

    upipe_zvbidec->flow_def_input = NULL;
    upipe_zvbidec->flow_def_sent = false;
    upipe_zvbidec->channel = UPIPE_ZVBIDEC_CC1;
    upipe_zvbidec->hsize = UINT64_MAX;
    upipe_zvbidec->vsize = UINT64_MAX;
    upipe_zvbidec->out_hsize = UINT64_MAX;
    upipe_zvbidec->out_vsize = UINT64_MAX;
    upipe_zvbidec->out_hposition = UINT64_MAX;
    upipe_zvbidec->out_vposition = UINT64_MAX;
    upipe_zvbidec->timestamp = 0;
    upipe_zvbidec->uref = NULL;
    upipe_zvbidec->page_dirty = false;

    upipe_zvbidec_init_urefcount(upipe);
    upipe_zvbidec_init_output(upipe);
    upipe_zvbidec_init_ubuf_mgr(upipe);

    upipe_throw_ready(upipe);
    return upipe;
}

/** @internal @This frees all resources allocated.
 *
 * @param upipe description structure of the pipe
 */
static void upipe_zvbidec_free(struct upipe *upipe)
{
    struct upipe_zvbidec *upipe_zvbidec = upipe_zvbidec_from_upipe(upipe);

    upipe_throw_dead(upipe);

    if (upipe_zvbidec->vbi != NULL)
        vbi_decoder_delete(upipe_zvbidec->vbi);
    uref_free(upipe_zvbidec->flow_def_input);

    upipe_zvbidec_clean_ubuf_mgr(upipe);
    upipe_zvbidec_clean_output(upipe);
    upipe_zvbidec_clean_urefcount(upipe);
    upipe_zvbidec_free_void(upipe);
}

static struct upipe_mgr upipe_zvbidec_mgr = {
    .refcount = NULL,
    .signature = UPIPE_ZVBIDEC_SIGNATURE,

    .upipe_alloc = upipe_zvbidec_alloc,
    .upipe_input = upipe_zvbidec_input,
    .upipe_control = upipe_zvbidec_control,

    .upipe_mgr_control = NULL
};

/** @This returns the management structure for zvbidec pipes.
 *
 * @return pointer to manager
 */
struct upipe_mgr *upipe_zvbidec_mgr_alloc(void)
{
    return &upipe_zvbidec_mgr;
}
