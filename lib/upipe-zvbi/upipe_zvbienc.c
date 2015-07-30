/*
 * Copyright (C) 2015 Open Broadcast Systems Ltd
 *
 * Authors: Kieran Kunhya
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston MA 02110-1301, USA.
 */

/** @file
 * @short Upipe libzvbi encoding module
 */

#include <upipe/ubase.h>
#include <upipe/uprobe.h>
#include <upipe/uref.h>
#include <upipe/uref_pic.h>
#include <upipe/uref_flow.h>
#include <upipe/upipe.h>
#include <upipe/upipe_helper_upipe.h>
#include <upipe/upipe_helper_urefcount.h>
#include <upipe/upipe_helper_void.h>
#include <upipe/upipe_helper_output.h>
#include <upipe/upipe_helper_input.h>
#include <upipe-zvbi/upipe_zvbienc.h>
#include <upipe/uref_pic_flow.h>

#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdarg.h>
#include <assert.h>

#include <libzvbi.h>

/** upipe_zvbienc structure */
struct upipe_zvbienc {
    /** refcount management structure */
    struct urefcount urefcount;

    /** temporary uref storage (used during urequest) */
    struct uchain urefs;
    /** nb urefs in storage */
    unsigned int nb_urefs;
    /** max urefs in storage */
    unsigned int max_urefs;
    /** list of blockers (used during udeal) */
    struct uchain blockers;

    /** output pipe */
    struct upipe *output;
    /** flow_definition packet */
    struct uref *flow_def;
    /** output state */
    enum upipe_helper_output_state output_state;
    /** list of output requests */
    struct uchain request_list;

    /** picture format: 0=pal, 1=ntsc */
    int pic_fmt;

    vbi_sampling_par sp;

    /** public upipe structure */
    struct upipe upipe;
};

/** @hidden */
static bool upipe_zvbienc_handle(struct upipe *upipe, struct uref *uref,
                                      struct upump **upump_p);

UPIPE_HELPER_UPIPE(upipe_zvbienc, upipe, UPIPE_ZVBIENC_SIGNATURE);
UPIPE_HELPER_UREFCOUNT(upipe_zvbienc, urefcount, upipe_zvbienc_free)
UPIPE_HELPER_VOID(upipe_zvbienc);
UPIPE_HELPER_OUTPUT(upipe_zvbienc, output, flow_def, output_state, request_list)
UPIPE_HELPER_INPUT(upipe_zvbienc, urefs, nb_urefs, max_urefs, blockers, upipe_zvbienc_handle)

static void upipe_zvbienc_process_captions(struct uref *uref, vbi_sliced *sliced,
                                           const uint8_t *pic_data, size_t pic_data_size)
{
    uint8_t process, cc_count;

    process = (pic_data[0] >> 6) & 1;
    if (!process)
        return;

    cc_count = 

    pic_data += 2;
    pic_data_size =- 2;

    if (cc_count*3 > pic_data_size)
        return;

    int i;
    for (i = 0; i < pic_data_size/3; i++) {
        uint8_t valid = (pic_data[3*i] >> 2) & 1;
        if (valid){
            uint8_t cc_type = pic_data[3*i] & 0x3;
            if (cc_type == 0) {
                sliced[0].data[0] = pic_data[3*i + 1];
                sliced[0].data[1] = pic_data[3*i + 2];
            }
            else if (cc_type == 1) {
                sliced[1].data[0] = pic_data[3*i + 1];
                sliced[1].data[1] = pic_data[3*i + 2];
            }
        }
    }
}

/** @internal @This handles data.
 *
 * @param upipe description structure of the pipe
 * @param uref uref structure describing the picture
 * @param upump_p reference to pump that generated the buffer
 * @return false if the input must be blocked
 */
static bool upipe_zvbienc_handle(struct upipe *upipe, struct uref *uref,
                                 struct upump **upump_p)
{
    struct upipe_zvbienc *upipe_zvbienc = upipe_zvbienc_from_upipe(upipe);
    uint8_t *buf;
    const uint8_t *pic_data = NULL;
    size_t pic_data_size = 0;

    vbi_sliced sliced[2];
    sliced[0].id = VBI_SLICED_CAPTION_525_F1;
    sliced[0].line = 21;
    sliced[0].data[0] = 0x80;
    sliced[0].data[1] = 0x80;
    sliced[1].id = VBI_SLICED_CAPTION_525_F2;
    sliced[1].line = 284;
    sliced[1].data[0] = 0x80;
    sliced[1].data[1] = 0x80;

    uref_pic_get_cea_708(uref, &pic_data, &pic_data_size);
    if( pic_data_size > 0 )
        upipe_zvbienc_process_captions(uref, sliced, pic_data, pic_data_size);

    uref_pic_plane_write(uref, "y8", 0, 1, -1, 2, &buf);
    int success = vbi_raw_video_image (buf, 720*2, &upipe_zvbienc->sp,
                                       0, 0, 0, 0x000000FF, false,
                                       sliced, 1);
    uref_pic_plane_unmap(uref, "y8", 0, 1, -1, 2);

    upipe_zvbienc_output(upipe, uref, upump_p);
    return true;
}

/** @internal @This receives incoming uref.
 *
 * @param upipe description structure of the pipe
 * @param uref uref structure describing the picture
 * @param upump_p reference to pump that generated the buffer
 */
static void upipe_zvbienc_input(struct upipe *upipe, struct uref *uref,
                                     struct upump **upump_p)
{
    if (!upipe_zvbienc_check_input(upipe)) {
        upipe_zvbienc_hold_input(upipe, uref);
        upipe_zvbienc_block_input(upipe, upump_p);
    } else if (!upipe_zvbienc_handle(upipe, uref, upump_p)) {
        upipe_zvbienc_hold_input(upipe, uref);
        upipe_zvbienc_block_input(upipe, upump_p);
        /* Increment upipe refcount to avoid disappearing before all packets
         * have been sent. */
        upipe_use(upipe);
    }
}

/** @internal @This sets the input flow definition.
 *
 * @param upipe description structure of the pipe
 * @param flow_def flow definition packet
 * @return an error code
 */
static int upipe_zvbienc_set_flow_def(struct upipe *upipe, struct uref *flow_def)
{
    if (flow_def == NULL)
        return UBASE_ERR_INVALID;
    struct uref *flow_def_dup;
    if (unlikely((flow_def_dup = uref_dup(flow_def)) == NULL))
        return UBASE_ERR_ALLOC;
    upipe_zvbienc_store_flow_def(upipe, flow_def_dup);
    return UBASE_ERR_NONE;
}

/** @internal @This gets the zvbienc pic_fmt.
 *
 * @param upipe description structure of the pipe
 * @param pic_fmt_p filled in with the zvbienc pic_fmt
 * @return an error code
 */
static int _upipe_zvbienc_get_pic_fmt(struct upipe *upipe, int *pic_fmt_p)
{
    struct upipe_zvbienc *upipe_zvbienc = upipe_zvbienc_from_upipe(upipe);
    *pic_fmt_p = upipe_zvbienc->pic_fmt;
    return UBASE_ERR_NONE;
}

/** @internal @This sets the zvbienc pic_fmt.
 *
 * @param upipe description structure of the pipe
 * @param pic_fmt zvbienc pic_fmt
 * @return an error code
 */
static int _upipe_zvbienc_set_pic_fmt(struct upipe *upipe, int pic_fmt)
{
    struct upipe_zvbienc *upipe_zvbienc = upipe_zvbienc_from_upipe(upipe);
    upipe_zvbienc->pic_fmt = pic_fmt;
    upipe_dbg_va(upipe, "setting pic_fmt to %s", pic_fmt ? "pal" : "ntsc");
    pic_fmt = 1;
    if (pic_fmt == 0) {
#if 0
        sp.scanning     = 625; /* PAL/SECAM */
        sp.sampling_format  = VBI_PIXFMT_YUV420;
        sp.sampling_rate    = 13.5e6;
        sp.bytes_per_line   = 720 * 2; /* 2 bpp */
        sp.offset       = 9.5e-6 * 13.5e6;
        sp.start[0]     = 23;
        sp.count[0]     = 17;
        sp.start[1]     = 319;
        sp.count[1]     = 17;
        sp.interlaced       = TRUE;
        sp.synchronous      = TRUE;
#endif
    }
    else if (pic_fmt == 1) {
        upipe_zvbienc->sp.scanning         = 525; /* NTSC */
        upipe_zvbienc->sp.sampling_format  = VBI_PIXFMT_YUV420;
        upipe_zvbienc->sp.sampling_rate    = 13.5e6;
        upipe_zvbienc->sp.bytes_per_line   = 720;
        upipe_zvbienc->sp.offset       = 122;
        upipe_zvbienc->sp.start[0]     = 21;
        upipe_zvbienc->sp.count[0]     = 1;
        upipe_zvbienc->sp.start[1]     = 284;
        upipe_zvbienc->sp.count[1]     = 1;
        upipe_zvbienc->sp.interlaced   = TRUE;
        upipe_zvbienc->sp.synchronous  = TRUE;
    }

    return UBASE_ERR_NONE;
}

/** @internal @This processes control commands on an zvbienc pipe.
 *
 * @param upipe description structure of the pipe
 * @param command type of command to process
 * @param args arguments of the command
 * @return an error code
 */
static int upipe_zvbienc_control(struct upipe *upipe, int command, va_list args)
{
    switch (command) {
        case UPIPE_REGISTER_REQUEST: {
            struct urequest *request = va_arg(args, struct urequest *);
            return upipe_zvbienc_alloc_output_proxy(upipe, request);
        }
        case UPIPE_UNREGISTER_REQUEST: {
            struct urequest *request = va_arg(args, struct urequest *);
            return upipe_zvbienc_free_output_proxy(upipe, request);
        }
        case UPIPE_GET_FLOW_DEF: {
            struct uref **p = va_arg(args, struct uref **);
            return upipe_zvbienc_get_flow_def(upipe, p);
        }
        case UPIPE_SET_FLOW_DEF: {
            struct uref *flow_def = va_arg(args, struct uref *);
            return upipe_zvbienc_set_flow_def(upipe, flow_def);
        }
        case UPIPE_GET_OUTPUT: {
            struct upipe **p = va_arg(args, struct upipe **);
            return upipe_zvbienc_get_output(upipe, p);
        }
        case UPIPE_SET_OUTPUT: {
            struct upipe *output = va_arg(args, struct upipe *);
            return upipe_zvbienc_set_output(upipe, output);
        }

        /* specific commands */
        case UPIPE_ZVBIENC_GET_PIC_FMT: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_ZVBIENC_SIGNATURE)
            int *pic_fmt_p = va_arg(args, int *);
            return _upipe_zvbienc_get_pic_fmt(upipe, pic_fmt_p);
        }
        case UPIPE_ZVBIENC_SET_PIC_FMT: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_ZVBIENC_SIGNATURE)
            int pic_fmt = va_arg(args, int);
            return _upipe_zvbienc_set_pic_fmt(upipe, pic_fmt);
        }

        default:
            return UBASE_ERR_UNHANDLED;
    }
}

/** @internal @This allocates an zvbienc pipe.
 *
 * @param mgr common management structure
 * @param uprobe structure used to raise events
 * @param signature signature of the pipe allocator
 * @param args optional arguments
 * @return pointer to upipe or NULL in case of allocation error
 */
static struct upipe *upipe_zvbienc_alloc(struct upipe_mgr *mgr,
                                              struct uprobe *uprobe,
                                              uint32_t signature, va_list args)
{
    struct upipe *upipe = upipe_zvbienc_alloc_void(mgr, uprobe, signature, args);
    if (unlikely(upipe == NULL))
        return NULL;

    vbi_set_log_fn ((VBI_LOG_NOTICE |
        VBI_LOG_WARNING |
        VBI_LOG_ERROR |
        VBI_LOG_INFO),
      vbi_log_on_stderr,
      /* user_data */ NULL);

    struct upipe_zvbienc *upipe_zvbienc = upipe_zvbienc_from_upipe(upipe);

    upipe_zvbienc_init_urefcount(upipe);
    upipe_zvbienc_init_output(upipe);
    upipe_zvbienc_init_input(upipe);

    upipe_throw_ready(upipe);
    return upipe;
}

/** @internal @This frees all resources allocated.
 *
 * @param upipe description structure of the pipe
 */
static void upipe_zvbienc_free(struct upipe *upipe)
{
    upipe_throw_dead(upipe);

    upipe_zvbienc_clean_input(upipe);
    upipe_zvbienc_clean_output(upipe);
    upipe_zvbienc_clean_urefcount(upipe);
    upipe_zvbienc_free_void(upipe);
}

static struct upipe_mgr upipe_zvbienc_mgr = {
    .refcount = NULL,
    .signature = UPIPE_ZVBIENC_SIGNATURE,

    .upipe_alloc = upipe_zvbienc_alloc,
    .upipe_input = upipe_zvbienc_input,
    .upipe_control = upipe_zvbienc_control,

    .upipe_mgr_control = NULL
};

/** @This returns the management structure for zvbienc pipes.
 *
 * @return pointer to manager
 */
struct upipe_mgr *upipe_zvbienc_mgr_alloc(void)
{
    return &upipe_zvbienc_mgr;
}
