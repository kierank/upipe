/*
 * Copyright (C) 2017 Open Broadcast Systems Ltd
 *
 * Authors: Rafaël Carré
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject
 * to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/** @file
 * @short Upipe module to split raw audio into RTP packets
 */

#include <stdlib.h>
#include <limits.h>

#include <upipe/upipe.h>
#include <upipe/uclock.h>
#include <upipe/uref_clock.h>
#include <upipe/upipe_helper_upipe.h>
#include <upipe/upipe_helper_urefcount.h>
#include <upipe/upipe_helper_void.h>
#include <upipe/upipe_helper_output.h>
#include <upipe/upipe_helper_flow.h>
#include <upipe/upipe_helper_flow_def.h>
#include <upipe/upipe_helper_ubuf_mgr.h>
#include <upipe/upipe_helper_input.h>

#include <upipe/uref_pic.h>
#include <upipe/uref_pic_flow.h>

#include <upipe-modules/upipe_row_split.h>

struct upipe_row_split {
    /** refcount management structure */
    struct urefcount urefcount;

    /* output stuff */
    /** pipe acting as output */
    struct upipe *output;
    /** output flow definition packet */
    struct uref *flow_def;
    /** output state */
    enum upipe_helper_output_state output_state;
    /** list of output requests */
    struct uchain request_list;

    /** ubuf manager */
    struct ubuf_mgr *ubuf_mgr;
    /** flow format packet */
    struct uref *flow_format;
    /** ubuf manager request */
    struct urequest ubuf_mgr_request;

    /** frame duration in ticks */
    uint64_t frame_duration;

    /** temporary uref storage (used during urequest) */
    struct uchain input_urefs;
    /** nb urefs in storage */
    unsigned int nb_urefs;
    /** max urefs in storage */
    unsigned int max_urefs;
    /** list of blockers (used during urequest) */
    struct uchain blockers;

    /** public upipe structure */
    struct upipe upipe;
};

/** @hidden */
static int upipe_row_split_check(struct upipe *upipe, struct uref *flow_format);

/** @hidden */
static bool upipe_row_split_handle(struct upipe *upipe, struct uref *uref,
                                  struct upump **upump_p);

UPIPE_HELPER_UPIPE(upipe_row_split, upipe, UPIPE_ROW_SPLIT_SIGNATURE)
UPIPE_HELPER_UREFCOUNT(upipe_row_split, urefcount, upipe_row_split_free)
UPIPE_HELPER_VOID(upipe_row_split)
UPIPE_HELPER_OUTPUT(upipe_row_split, output, flow_def, output_state,
                    request_list)
UPIPE_HELPER_UBUF_MGR(upipe_row_split, ubuf_mgr, flow_format, ubuf_mgr_request,
                      upipe_row_split_check,
                      upipe_row_split_register_output_request,
                      upipe_row_split_unregister_output_request)
UPIPE_HELPER_INPUT(upipe_row_split, input_urefs, nb_urefs, max_urefs, blockers,
        upipe_row_split_handle)

static int upipe_row_split_check(struct upipe *upipe, struct uref *flow_format)
{
    if (flow_format)
        upipe_row_split_store_flow_def(upipe, flow_format);

    bool was_buffered = !upipe_row_split_check_input(upipe);
    upipe_row_split_output_input(upipe);
    upipe_row_split_unblock_input(upipe);
    if (was_buffered && upipe_row_split_check_input(upipe)) {
        /* All packets have been output, release again the pipe that has been
         * used in @ref upipe_row_split_input. */
        upipe_release(upipe);
    }

    return UBASE_ERR_NONE;
}

static int upipe_row_split_set_flow_def(struct upipe *upipe,
                                       struct uref *flow_def)
{
    struct upipe_row_split *upipe_row_split = upipe_row_split_from_upipe(upipe);

    if (flow_def == NULL)
        return UBASE_ERR_INVALID;
    UBASE_RETURN(uref_flow_match_def(flow_def, "pic."))

    struct urational fps;
    UBASE_RETURN(uref_pic_flow_get_fps(flow_def, &fps));
    upipe_row_split->frame_duration = UCLOCK_FREQ * fps.den / fps.num;

    struct uref *flow_def_dup = uref_dup(flow_def);
    if (unlikely(flow_def_dup == NULL)) {
        upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
        return UBASE_ERR_ALLOC;
    }

    upipe_row_split_require_ubuf_mgr(upipe, flow_def_dup);

    return UBASE_ERR_NONE;
}

static int upipe_row_split_control(struct upipe *upipe, int command,
                                  va_list args)
{
    switch (command) {
        case UPIPE_REGISTER_REQUEST: {
            struct urequest *request = va_arg(args, struct urequest *);
            return upipe_row_split_alloc_output_proxy(upipe, request);
        }
        case UPIPE_UNREGISTER_REQUEST: {
            struct urequest *request = va_arg(args, struct urequest *);
            return upipe_row_split_free_output_proxy(upipe, request);
        }
        case UPIPE_SET_FLOW_DEF: {
            struct uref *flow_def = va_arg(args, struct uref *);
            return upipe_row_split_set_flow_def(upipe, flow_def);
        }
        case UPIPE_GET_FLOW_DEF:
        case UPIPE_GET_OUTPUT:
        case UPIPE_SET_OUTPUT:
            return upipe_row_split_control_output(upipe, command, args);
        default:
            return UBASE_ERR_UNHANDLED;
    }
}

static void upipe_row_split_free(struct upipe *upipe)
{
    upipe_row_split_clean_ubuf_mgr(upipe);
    upipe_row_split_clean_urefcount(upipe);
    upipe_row_split_clean_output(upipe);
    upipe_row_split_clean_input(upipe);
    upipe_row_split_free_void(upipe);
}

static struct upipe *upipe_row_split_alloc(struct upipe_mgr *mgr,
                                           struct uprobe *uprobe,
                                           uint32_t signature,
                                           va_list args)
{
    struct upipe *upipe =
        upipe_row_split_alloc_void(mgr, uprobe, signature, args);
    if (unlikely(upipe == NULL))
        return NULL;
    struct upipe_row_split *upipe_row_split = upipe_row_split_from_upipe(upipe);

    upipe_row_split_init_urefcount(upipe);
    upipe_row_split_init_input(upipe);
    upipe_row_split_init_ubuf_mgr(upipe);
    upipe_row_split_init_output(upipe);

    return upipe;
}

static void upipe_row_split_input(struct upipe *upipe, struct uref *uref,
                                  struct upump **upump_p)
{
    if (!upipe_row_split_check_input(upipe)) {
        upipe_row_split_hold_input(upipe, uref);
        upipe_row_split_block_input(upipe, upump_p);
    } else if (!upipe_row_split_handle(upipe, uref, upump_p)) {
        upipe_row_split_hold_input(upipe, uref);
        upipe_row_split_block_input(upipe, upump_p);
        /* Increment upipe refcount to avoid disappearing before all packets
         * have been sent. */
        upipe_use(upipe);
    }
}

static bool upipe_row_split_handle(struct upipe *upipe, struct uref *uref,
                                  struct upump **upump_p)
{
    struct upipe_row_split *upipe_row_split = upipe_row_split_from_upipe(upipe);
    if (!upipe_row_split->ubuf_mgr)
        return false;

    size_t hsize, vsize;
    uint8_t macropixel;
    if (unlikely(!ubase_check(uref_pic_size(uref, &hsize, &vsize,
                        &macropixel)))) {
        upipe_warn(upipe, "dropping picture");
        upipe_throw_error(upipe, UBASE_ERR_INVALID);
        uref_free(uref);
        return true;
    }

    uint64_t original_height = vsize;
    uint64_t vsize_slice = 16;
    uint64_t done = 0;
    while (vsize) {
        if (vsize_slice > vsize)
            vsize_slice = vsize;
        struct ubuf *ubuf = ubuf_pic_alloc(upipe_row_split->ubuf_mgr, hsize, vsize_slice);
        if (!ubuf)
            abort();

        const char *chroma = NULL;
        while (ubase_check(ubuf_pic_plane_iterate(ubuf, &chroma)) &&
                chroma != NULL) {
            const uint8_t *src;
            uint8_t *dst;
            if (!ubase_check(ubuf_pic_plane_write(ubuf, chroma, 0, 0, -1, -1, &dst))) {
                abort();
            }

            if (!ubase_check(uref_pic_plane_read(uref, chroma, 0, done, -1, vsize_slice, &src))) {
                abort();
            }

            size_t dst_stride;
            uint8_t dst_hsub, dst_vsub, dst_macropixel_size;
            if (unlikely(!ubase_check(ubuf_pic_plane_size(ubuf, chroma, &dst_stride,
                        &dst_hsub, &dst_vsub, &dst_macropixel_size)))) {
                abort();
            }

            memcpy(dst, src, vsize_slice * dst_stride);
            // TODO: dst stride != src stride

            ubase_assert(uref_pic_plane_unmap(uref, chroma, 0, done, -1, vsize_slice));
            ubase_assert(ubuf_pic_plane_unmap(ubuf, chroma, 0, 0, -1, -1));

        }

        struct uref *uref_slice = uref_fork(uref, ubuf);

        uref_attr_set_unsigned(uref_slice,
                original_height, UDICT_TYPE_UNSIGNED, "original_height");
        uref_attr_set_unsigned(uref_slice,
                (vsize_slice * upipe_row_split->frame_duration) / original_height,
                UDICT_TYPE_UNSIGNED, "fraction_duration");

        uref_pic_set_vposition(uref_slice, done);

        uint64_t pts = 0;
        uref_clock_get_pts_sys(uref_slice, &pts);
        pts += (done * upipe_row_split->frame_duration) / vsize;
        uref_clock_set_pts_sys(uref_slice, pts);

        upipe_row_split_output(upipe, uref_slice, NULL);

        vsize -= vsize_slice;
        done += vsize_slice;
    }

    uref_free(uref);

    return true;
}

static struct upipe_mgr upipe_row_split_mgr = {
    .refcount = NULL,
    .signature = UPIPE_ROW_SPLIT_SIGNATURE,

    .upipe_alloc = upipe_row_split_alloc,
    .upipe_input = upipe_row_split_input,
    .upipe_control = upipe_row_split_control,

    .upipe_mgr_control = NULL,
};

struct upipe_mgr *upipe_row_split_mgr_alloc(void)
{
    return &upipe_row_split_mgr;
}
