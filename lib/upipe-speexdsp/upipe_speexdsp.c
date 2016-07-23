/*
 * Copyright (C) 2016 Open Broadcast Systems Ltd
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
 * @short Upipe speexdsp (ffmpeg) module
 */

#include <upipe/ubase.h>
#include <upipe/uprobe.h>
#include <upipe/uref.h>
#include <upipe/ubuf.h>
#include <upipe/uref_clock.h>
#include <upipe/uref_sound.h>
#include <upipe/uref_sound_flow.h>
#include <upipe/uref_dump.h>
#include <upipe/uclock.h>
#include <upipe/upipe.h>
#include <upipe/upipe_helper_upipe.h>
#include <upipe/upipe_helper_urefcount.h>
#include <upipe/upipe_helper_void.h>
#include <upipe/upipe_helper_flow_def.h>
#include <upipe/upipe_helper_ubuf_mgr.h>
#include <upipe/upipe_helper_output.h>
#include <upipe/upipe_helper_input.h>
#include <upipe-av/upipe_av_samplefmt.h>
#include <upipe-speexdsp/upipe_speexdsp.h>

#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>

#include <speex/speex_resampler.h>

/** @hidden */
static bool upipe_speexdsp_handle(struct upipe *upipe, struct uref *uref,
                             struct upump **upump_p);
/** @hidden */
static int upipe_speexdsp_check(struct upipe *upipe, struct uref *flow_format);

/** upipe_speexdsp structure with speexdsp parameters */ 
struct upipe_speexdsp {
    /** refcount management structure */
    struct urefcount urefcount;

    /** input flow */
    struct uref *flow_def_input;
    /** attributes added by the pipe */
    struct uref *flow_def_attr;
    /** output flow */
    struct uref *flow_def;
    /** output state */
    enum upipe_helper_output_state output_state;
    /** list of output requests */
    struct uchain request_list;
    /** output pipe */
    struct upipe *output;

    /** ubuf manager */
    struct ubuf_mgr *ubuf_mgr;
    /** flow format packet */
    struct uref *flow_format;
    /** ubuf manager request */
    struct urequest ubuf_mgr_request;

    /** temporary uref storage (used during urequest) */
    struct uchain urefs;
    /** nb urefs in storage */
    unsigned int nb_urefs;
    /** max urefs in storage */
    unsigned int max_urefs;
    /** list of blockers (used during udeal) */
    struct uchain blockers;

    /** speexdsp context */
    SpeexResamplerState *ctx;

    /** current drift rate */
    struct urational drift_rate;

    /** public upipe structure */
    struct upipe upipe;
};

UPIPE_HELPER_UPIPE(upipe_speexdsp, upipe, UPIPE_SPEEXDSP_SIGNATURE);
UPIPE_HELPER_UREFCOUNT(upipe_speexdsp, urefcount, upipe_speexdsp_free);
UPIPE_HELPER_VOID(upipe_speexdsp)
UPIPE_HELPER_OUTPUT(upipe_speexdsp, output, flow_def, output_state, request_list)
UPIPE_HELPER_FLOW_DEF(upipe_speexdsp, flow_def_input, flow_def_attr)
UPIPE_HELPER_UBUF_MGR(upipe_speexdsp, ubuf_mgr, flow_format, ubuf_mgr_request,
                      upipe_speexdsp_check,
                      upipe_speexdsp_register_output_request,
                      upipe_speexdsp_unregister_output_request)
UPIPE_HELPER_INPUT(upipe_speexdsp, urefs, nb_urefs, max_urefs, blockers, upipe_speexdsp_handle)


/** @internal */
static void resample_audio(struct upipe *upipe, struct uref *uref)
{
    struct upipe_speexdsp *upipe_speexdsp =
        upipe_speexdsp_from_upipe(upipe);

    struct urational drift_rate;
    if (!ubase_check(uref_clock_get_rate(uref, &drift_rate)))
        drift_rate = (struct urational){ 1, 1 };

    /* reinitialize resampler when drift rate changes */
    if (urational_cmp(&drift_rate, &upipe_speexdsp->drift_rate)) {
        upipe_speexdsp->drift_rate = drift_rate;
        spx_uint32_t ratio_num = drift_rate.den;
        spx_uint32_t ratio_den = drift_rate.num;
        spx_uint32_t in_rate = 48000 * ratio_num / ratio_den;
        spx_uint32_t out_rate = 48000;
        int ret = speex_resampler_set_rate_frac(upipe_speexdsp->ctx,
                ratio_num, ratio_den, in_rate, out_rate);
        assert(!ret);
        upipe_notice_va(upipe, "\t\t\tREINIT resampler(%u, %u, %u, %u)",
                ratio_num, ratio_den,
                in_rate, out_rate);
    }

    size_t size;
    if (!ubase_check(uref_sound_size(uref, &size, NULL /* sample_size */)))
        return;

    struct ubuf *ubuf = ubuf_sound_alloc(uref->ubuf->mgr, size + 10);
    assert(ubuf);

    const float *in;
    uref_sound_read_float(uref, 0, size, &in, 1);

    float *out;
    ubuf_sound_write_float(ubuf, 0, size + 10, &out, 1);

    spx_uint32_t in_len = size;         /* input size */
    spx_uint32_t out_len = size + 10;   /* available output size */

    int ret = speex_resampler_process_interleaved_float(upipe_speexdsp->ctx,
            in, &in_len, out, &out_len);
    assert(ret == 0);

    ubuf_sound_resize(ubuf, 0, out_len);

    uref_sound_unmap(uref, 0, size, 1);
    ubuf_sound_unmap(ubuf, 0, out_len, 1);
    uref_attach_ubuf(uref, ubuf);
}

/** @internal @This handles data.
 *
 * @param upipe description structure of the pipe
 * @param uref uref structure
 * @param upump_p reference to pump that generated the buffer
 * @return false if the input must be blocked
 */
static bool upipe_speexdsp_handle(struct upipe *upipe, struct uref *uref,
                             struct upump **upump_p)
{
    struct upipe_speexdsp *upipe_speexdsp = upipe_speexdsp_from_upipe(upipe);
    const char *def;

    resample_audio(upipe, uref);

    upipe_speexdsp_output(upipe, uref, upump_p);
    return true;
}

/** @internal @This receives incoming uref.
 *
 * @param upipe description structure of the pipe
 * @param uref uref structure describing the picture
 * @param upump_p reference to pump that generated the buffer
 */
static void upipe_speexdsp_input(struct upipe *upipe, struct uref *uref,
                            struct upump **upump_p)
{
    if (!upipe_speexdsp_check_input(upipe)) {
        upipe_speexdsp_hold_input(upipe, uref);
        upipe_speexdsp_block_input(upipe, upump_p);
    } else if (!upipe_speexdsp_handle(upipe, uref, upump_p)) {
        upipe_speexdsp_hold_input(upipe, uref);
        upipe_speexdsp_block_input(upipe, upump_p);
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
static int upipe_speexdsp_check(struct upipe *upipe, struct uref *flow_format)
{
    struct upipe_speexdsp *upipe_speexdsp = upipe_speexdsp_from_upipe(upipe);
    if (flow_format != NULL)
        upipe_speexdsp_store_flow_def(upipe, flow_format);

    if (upipe_speexdsp->flow_def == NULL)
        return UBASE_ERR_NONE;

    bool was_buffered = !upipe_speexdsp_check_input(upipe);
    upipe_speexdsp_output_input(upipe);
    upipe_speexdsp_unblock_input(upipe);
    if (was_buffered && upipe_speexdsp_check_input(upipe)) {
        /* All packets have been output, release again the pipe that has been
         * used in @ref upipe_speexdsp_input. */
        upipe_release(upipe);
    }
    return UBASE_ERR_NONE;
}

/** @internal @This sets the input flow definition.
 *
 * @param upipe description structure of the pipe
 * @param flow_def flow definition packet
 * @return an error code
 */
static int upipe_speexdsp_set_flow_def(struct upipe *upipe, struct uref *flow_def)
{
    struct upipe_speexdsp *upipe_speexdsp = upipe_speexdsp_from_upipe(upipe);

    if (flow_def == NULL)
        return UBASE_ERR_INVALID;

    const char *def;
    uint8_t in_planes;
    UBASE_RETURN(uref_flow_get_def(flow_def, &def))
    if (unlikely(ubase_ncmp(def, "sound.f32.") ||
                 !ubase_check(uref_sound_flow_get_planes(flow_def,
                                                         &in_planes))))
        return UBASE_ERR_INVALID;

    uint64_t rate;
    if (!ubase_check(uref_sound_flow_get_rate(flow_def, &rate))) {
        upipe_err(upipe, "incompatible flow def");
        uref_dump(flow_def, upipe->uprobe);
        return UBASE_ERR_INVALID;
    }

    uint8_t channels;
    if (unlikely(!ubase_check(uref_sound_flow_get_channels(flow_def,
                        &channels))))
        return UBASE_ERR_INVALID;

    flow_def = uref_dup(flow_def);
    if (unlikely(flow_def == NULL)) {
        upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
        return UBASE_ERR_ALLOC;
    }

    upipe_speexdsp_store_flow_def(upipe, flow_def);

    if (upipe_speexdsp->ctx)
        speex_resampler_destroy(upipe_speexdsp->ctx);

    int err;
    upipe_speexdsp->ctx = speex_resampler_init(channels,
                rate, rate, SPEEX_RESAMPLER_QUALITY_MAX, &err);
    if (!upipe_speexdsp->ctx || err)
        return UBASE_ERR_INVALID;

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
static int upipe_speexdsp_control(struct upipe *upipe, int command, va_list args)
{
    switch (command) {
        /* generic commands */
        case UPIPE_REGISTER_REQUEST: {
            struct urequest *request = va_arg(args, struct urequest *);
            if (request->type == UREQUEST_UBUF_MGR ||
                request->type == UREQUEST_FLOW_FORMAT)
                return upipe_throw_provide_request(upipe, request);
            return upipe_speexdsp_alloc_output_proxy(upipe, request);
        }
        case UPIPE_UNREGISTER_REQUEST: {
            struct urequest *request = va_arg(args, struct urequest *);
            if (request->type == UREQUEST_UBUF_MGR ||
                request->type == UREQUEST_FLOW_FORMAT)
                return UBASE_ERR_NONE;
            return upipe_speexdsp_free_output_proxy(upipe, request);
        }

        case UPIPE_GET_OUTPUT: {
            struct upipe **p = va_arg(args, struct upipe **);
            return upipe_speexdsp_get_output(upipe, p);
        }
        case UPIPE_SET_OUTPUT: {
            struct upipe *output = va_arg(args, struct upipe *);
            return upipe_speexdsp_set_output(upipe, output);
        }
        case UPIPE_GET_FLOW_DEF: {
            struct uref **p = va_arg(args, struct uref **);
            return upipe_speexdsp_get_flow_def(upipe, p);
        }
        case UPIPE_SET_FLOW_DEF: {
            struct uref *flow = va_arg(args, struct uref *);
            return upipe_speexdsp_set_flow_def(upipe, flow);
        }

        default:
            return UBASE_ERR_UNHANDLED;
    }
}

/** @internal @This allocates a speexdsp pipe.
 *
 * @param mgr common management structure
 * @param uprobe structure used to raise events
 * @param signature signature of the pipe allocator
 * @param args optional arguments
 * @return pointer to upipe or NULL in case of allocation error
 */
static struct upipe *upipe_speexdsp_alloc(struct upipe_mgr *mgr,
                                     struct uprobe *uprobe,
                                     uint32_t signature, va_list args)
{
    struct upipe *upipe = upipe_speexdsp_alloc_void(mgr, uprobe, signature,
                                               args);
    if (unlikely(upipe == NULL))
        return NULL;

    upipe_speexdsp_init_urefcount(upipe);
    upipe_speexdsp_init_ubuf_mgr(upipe);
    upipe_speexdsp_init_output(upipe);
    upipe_speexdsp_init_flow_def(upipe);
    upipe_speexdsp_init_input(upipe);

    upipe_throw_ready(upipe);
    return upipe;
}

/** @This frees a upipe.
 *
 * @param upipe description structure of the pipe
 */
static void upipe_speexdsp_free(struct upipe *upipe)
{
    struct upipe_speexdsp *upipe_speexdsp = upipe_speexdsp_from_upipe(upipe);
    if (likely(upipe_speexdsp->ctx))
        speex_resampler_destroy(upipe_speexdsp->ctx);

    upipe_throw_dead(upipe);
    upipe_speexdsp_clean_input(upipe);
    upipe_speexdsp_clean_output(upipe);
    upipe_speexdsp_clean_flow_def(upipe);
    upipe_speexdsp_clean_ubuf_mgr(upipe);
    upipe_speexdsp_clean_urefcount(upipe);
    upipe_speexdsp_free_void(upipe);
}

/** module manager static descriptor */
static struct upipe_mgr upipe_speexdsp_mgr = {
    .refcount = NULL,
    .signature = UPIPE_SPEEXDSP_SIGNATURE,

    .upipe_alloc = upipe_speexdsp_alloc,
    .upipe_input = upipe_speexdsp_input,
    .upipe_control = upipe_speexdsp_control,

    .upipe_mgr_control = NULL
};

/** @This returns the management structure for speexdsp pipes
 *
 * @return pointer to manager
 */
struct upipe_mgr *upipe_speexdsp_mgr_alloc(void)
{
    return &upipe_speexdsp_mgr;
}

