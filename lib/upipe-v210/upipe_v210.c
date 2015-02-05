/*
 * V210 encoder
 *
 * Copyright (C) 2009 Michael Niedermayer <michaelni@gmx.at>
 * Copyright (c) 2009 Baptiste Coudurier <baptiste dot coudurier at gmail dot com>
 *
 * This file is part of FFmpeg.
 *
 * FFmpeg is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * FFmpeg is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with FFmpeg; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

/** @file
 * @short Upipe v210 module
 */

#include "upipe_v210.h"

/** @hidden */
static bool upipe_v210_handle(struct upipe *upipe, struct uref *uref,
                             struct upump **upump_p);
/** @hidden */
static int upipe_v210_check(struct upipe *upipe, struct uref *flow_format);


UPIPE_HELPER_UPIPE(upipe_v210, upipe, UPIPE_V210_SIGNATURE);
UPIPE_HELPER_UREFCOUNT(upipe_v210, urefcount, upipe_v210_free)
UPIPE_HELPER_FLOW(upipe_v210, "pic.");
UPIPE_HELPER_OUTPUT(upipe_v210, output, flow_def, output_state, request_list)
UPIPE_HELPER_FLOW_DEF(upipe_v210, flow_def_input, flow_def_attr)
UPIPE_HELPER_UBUF_MGR(upipe_v210, ubuf_mgr, flow_format, ubuf_mgr_request,
                      upipe_v210_check,
                      upipe_v210_register_output_request,
                      upipe_v210_unregister_output_request)
UPIPE_HELPER_INPUT(upipe_v210, urefs, nb_urefs, max_urefs, blockers, upipe_v210_handle)


#define CLIP(v) av_clip(v, 4, 1019)
#define CLIP8(v) av_clip(v, 1, 254)

#define WRITE_PIXELS(a, b, c)           \
    do {                                \
        val =   CLIP(*a++);             \
        val |= (CLIP(*b++) << 10) |     \
               (CLIP(*c++) << 20);      \
        AV_WL32(dst, val);              \
        dst += 4;                       \
    } while (0)

#define WRITE_PIXELS8(a, b, c)          \
    do {                                \
        val =  (CLIP8(*a++) << 2);       \
        val |= (CLIP8(*b++) << 12) |     \
               (CLIP8(*c++) << 22);      \
        AV_WL32(dst, val);              \
        dst += 4;                       \
    } while (0)

static void v210_planar_pack_8_c(const uint8_t *y, const uint8_t *u,
                                 const uint8_t *v, uint8_t *dst, ptrdiff_t width)
{
    uint32_t val;
    int i;

    /* unroll this to match the assembly */
    for( i = 0; i < width-11; i += 12 ){
        WRITE_PIXELS8(u, y, v);
        WRITE_PIXELS8(y, u, y);
        WRITE_PIXELS8(v, y, u);
        WRITE_PIXELS8(y, v, y);
        WRITE_PIXELS8(u, y, v);
        WRITE_PIXELS8(y, u, y);
        WRITE_PIXELS8(v, y, u);
        WRITE_PIXELS8(y, v, y);
    }
}

static void v210_planar_pack_10_c(const uint16_t *y, const uint16_t *u,
                                  const uint16_t *v, uint8_t *dst, ptrdiff_t width)
{
    uint32_t val;
    int i;

    for( i = 0; i < width-5; i += 6 ){
        WRITE_PIXELS(u, y, v);
        WRITE_PIXELS(y, u, y);
        WRITE_PIXELS(v, y, u);
        WRITE_PIXELS(y, v, y);
    }
}

/** @internal @This handles data.
 *
 * @param upipe description structure of the pipe
 * @param uref uref structure describing the picture
 * @param upump_p reference to pump that generated the buffer
 * @return false if the input must be blocked
 */
static bool upipe_v210_handle(struct upipe *upipe, struct uref *uref,
                             struct upump **upump_p)
{
    struct upipe_v210 *upipe_v210 = upipe_v210_from_upipe(upipe);
    const char *def;
    if (unlikely(ubase_check(uref_flow_get_def(uref, &def)))) {
        upipe_v210_store_flow_def(upipe, NULL);
        uref = upipe_v210_store_flow_def_input(upipe, uref);
        upipe_v210_require_ubuf_mgr(upipe, uref);
        return true;
    }

    if (upipe_v210->flow_def == NULL)
        return false;

    upipe_v210_output(upipe, uref, upump_p);
    return true;
}

/** @internal @This receives incoming uref.
 *
 * @param upipe description structure of the pipe
 * @param uref uref structure describing the picture
 * @param upump_p reference to pump that generated the buffer
 */
static void upipe_v210_input(struct upipe *upipe, struct uref *uref,
                            struct upump **upump_p)
{
    if (!upipe_v210_check_input(upipe)) {
        upipe_v210_hold_input(upipe, uref);
        upipe_v210_block_input(upipe, upump_p);
    } else if (!upipe_v210_handle(upipe, uref, upump_p)) {
        upipe_v210_hold_input(upipe, uref);
        upipe_v210_block_input(upipe, upump_p);
        /* Increment upipe refcount to avoid disappearing before all packets
         * have been sent. */
        upipe_use(upipe);
    }
}

/** @internal @This receives a provided ubuf manager.
 *
 * @param upipe description structure of the pipe
 * @param flow_format amended flow format
 * @return an error code
 */
static int upipe_v210_check(struct upipe *upipe, struct uref *flow_format)
{
    struct upipe_v210 *upipe_v210 = upipe_v210_from_upipe(upipe);
    if (flow_format != NULL)
        upipe_v210_store_flow_def(upipe, flow_format);

    if (upipe_v210->flow_def == NULL)
        return UBASE_ERR_NONE;

    bool was_buffered = !upipe_v210_check_input(upipe);
    upipe_v210_output_input(upipe);
    upipe_v210_unblock_input(upipe);
    if (was_buffered && upipe_v210_check_input(upipe)) {
        /* All packets have been output, release again the pipe that has been
         * used in @ref upipe_v210_input. */
        upipe_release(upipe);
    }
    return UBASE_ERR_NONE;
}

/** @internal @This requires a ubuf manager by proxy, and amends the flow
 * format.
 *
 * @param upipe description structure of the pipe
 * @param request description structure of the request
 * @return an error code
 */
static int upipe_v210_amend_ubuf_mgr(struct upipe *upipe,
                                    struct urequest *request)
{
    struct uref *flow_format = uref_dup(request->uref);
    UBASE_ALLOC_RETURN(flow_format);

    struct urequest ubuf_mgr_request;
    urequest_set_opaque(&ubuf_mgr_request, request);
    urequest_init_ubuf_mgr(&ubuf_mgr_request, flow_format,
                           upipe_v210_provide_output_proxy, NULL);
    upipe_throw_provide_request(upipe, &ubuf_mgr_request);
    urequest_clean(&ubuf_mgr_request);
    return UBASE_ERR_NONE;
}

/** @internal @This sets the input flow definition.
 *
 * @param upipe description structure of the pipe
 * @param flow_def flow definition packet
 * @return an error code
 */
static int upipe_v210_set_flow_def(struct upipe *upipe, struct uref *flow_def)
{
    if (flow_def == NULL)
        return UBASE_ERR_INVALID;

    UBASE_RETURN(uref_flow_match_def(flow_def, "pic."))

    // Check flow def

    upipe_input(upipe, flow_def, NULL);
    return UBASE_ERR_NONE;
}

/** @internal @This processes control commands on a file source pipe, and
 * checks the status of the pipe afterwards.
 *
 * @param upipe description structure of the pipe
 * @param command type of command to process
 * @param args arguments of the command
 * @return an error code
 */
static int upipe_v210_control(struct upipe *upipe, int command, va_list args)
{
    switch (command) {
        case UPIPE_REGISTER_REQUEST: {
            struct urequest *request = va_arg(args, struct urequest *);
            if (request->type == UREQUEST_UBUF_MGR)
                return upipe_v210_amend_ubuf_mgr(upipe, request);
            if (request->type == UREQUEST_FLOW_FORMAT)
                return upipe_throw_provide_request(upipe, request);
            return upipe_v210_alloc_output_proxy(upipe, request);
        }
        case UPIPE_UNREGISTER_REQUEST: {
            struct urequest *request = va_arg(args, struct urequest *);
            if (request->type == UREQUEST_UBUF_MGR ||
                request->type == UREQUEST_FLOW_FORMAT)
                return UBASE_ERR_NONE;
            return upipe_v210_free_output_proxy(upipe, request);
        }

        case UPIPE_GET_OUTPUT: {
            struct upipe **p = va_arg(args, struct upipe **);
            return upipe_v210_get_output(upipe, p);
        }
        case UPIPE_SET_OUTPUT: {
            struct upipe *output = va_arg(args, struct upipe *);
            return upipe_v210_set_output(upipe, output);
        }
        case UPIPE_GET_FLOW_DEF: {
            struct uref **p = va_arg(args, struct uref **);
            return upipe_v210_get_flow_def(upipe, p);
        }
        case UPIPE_SET_FLOW_DEF: {
            struct uref *flow = va_arg(args, struct uref *);
            return upipe_v210_set_flow_def(upipe, flow);
        }
        default:
            return UBASE_ERR_UNHANDLED;
    }
}

/** @internal @This allocates a v210 pipe.
 *
 * @param mgr common management structure
 * @param uprobe structure used to raise events
 * @param signature signature of the pipe allocator
 * @param args optional arguments
 * @return pointer to upipe or NULL in case of allocation error
 */
static struct upipe *upipe_v210_alloc(struct upipe_mgr *mgr,
                                     struct uprobe *uprobe,
                                     uint32_t signature, va_list args)
{
    struct uref *flow_def;
    struct upipe *upipe = upipe_v210_alloc_flow(mgr, uprobe, signature,
                                               args, &flow_def);
    if (unlikely(upipe == NULL))
        return NULL;

    struct upipe_v210 *upipe_v210 = upipe_v210_from_upipe(upipe);
    upipe_v210->cpu_flags = av_get_cpu_flags();

    // CHECK INPUT PIXFMT    

    upipe_v210_init_urefcount(upipe);
    upipe_v210_init_ubuf_mgr(upipe);
    upipe_v210_init_output(upipe);
    upipe_v210_init_flow_def(upipe);
    upipe_v210_init_input(upipe);

    upipe_throw_ready(upipe);

    UBASE_FATAL(upipe, uref_pic_flow_set_align(flow_def, 16))
    upipe_v210_store_flow_def_attr(upipe, flow_def);
    return upipe;

fail:

    return NULL;
}

/** @This frees a upipe.
 *
 * @param upipe description structure of the pipe
 */
static void upipe_v210_free(struct upipe *upipe)
{
    struct upipe_v210 *upipe_v210 = upipe_v210_from_upipe(upipe);

    upipe_throw_dead(upipe);
    upipe_v210_clean_input(upipe);
    upipe_v210_clean_output(upipe);
    upipe_v210_clean_flow_def(upipe);
    upipe_v210_clean_ubuf_mgr(upipe);
    upipe_v210_clean_urefcount(upipe);
    upipe_v210_free_flow(upipe);
}

/** module manager static descriptor */
static struct upipe_mgr upipe_v210_mgr = {
    .refcount = NULL,
    .signature = UPIPE_V210_SIGNATURE,

    .upipe_alloc = upipe_v210_alloc,
    .upipe_input = upipe_v210_input,
    .upipe_control = upipe_v210_control,

    .upipe_mgr_control = NULL
};

/** @This returns the management structure for v210 pipes
 *
 * @return pointer to manager
 */
struct upipe_mgr *upipe_v210_mgr_alloc(void)
{
    return &upipe_v210_mgr;
}

