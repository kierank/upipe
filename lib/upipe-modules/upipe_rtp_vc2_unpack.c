/*
 * Copyright (C) 2016-2017 Open Broadcast Systems Ltd
 *
 * Authors: Rafaël Carré
 *          James Darnley
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
 * @short Upipe module to decapsulate RTP VC2 video
 */


#include <stdlib.h>
#include <limits.h>

#include <upipe/upipe.h>
#include <upipe/uclock.h>
#include <upipe/uref_clock.h>
#include <upipe/upipe_helper_upipe.h>
#include <upipe/upipe_helper_urefcount.h>
#include <upipe/upipe_helper_void.h>
#include <upipe/upipe_helper_uref_stream.h>
#include <upipe/upipe_helper_output.h>
#include <upipe/upipe_helper_flow.h>
#include <upipe/upipe_helper_flow_def.h>
#include <upipe/upipe_helper_ubuf_mgr.h>
#include <upipe/upipe_helper_input.h>
#include <upipe-modules/upipe_rtp_decaps.h>
#include <upipe/ubuf_block.h>

#include <bitstream/ietf/rtp.h>
#include <libavutil/intreadwrite.h>

#include <upipe-modules/upipe_rtp_vc2_unpack.h>

/**
 * Dirac Specification ->
 * 9.6 Parse Info Header Syntax. parse_info()
 * 4 byte start code + byte parse code + 4 byte size + 4 byte previous size
 */
#define PARSE_INFO_HEADER_SIZE 13

enum DiracParseCodes {
    DIRAC_PCODE_SEQ_HEADER      = 0x00,
    DIRAC_PCODE_END_SEQ         = 0x10,
    DIRAC_PCODE_AUX             = 0x20,
    DIRAC_PCODE_PAD             = 0x30,
    DIRAC_PCODE_PICTURE_HQ      = 0xE8,
    DIRAC_PCODE_PICTURE_FRAGMENT_HQ = 0xEC,
    DIRAC_PCODE_MAGIC           = 0x42424344,
};

struct upipe_rtp_vc2_unpack {
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

    /** temporary uref storage (used during urequest) */
    struct uchain urefs;
    /** nb urefs in storage */
    unsigned int nb_urefs;
    /** max urefs in storage */
    unsigned int max_urefs;
    /** list of blockers (used during urequest) */
    struct uchain blockers;

    /** public upipe structure */
    struct upipe upipe;

    uint32_t prev_offset;
};

/** @hidden */
static int upipe_rtp_vc2_unpack_check(struct upipe *upipe, struct uref *flow_format);

/** @hidden */
static bool upipe_rtp_vc2_unpack_handle(struct upipe *upipe, struct uref *uref,
                                  struct upump **upump_p);

UPIPE_HELPER_UPIPE(upipe_rtp_vc2_unpack, upipe, UPIPE_RTP_VC2_UNPACK_SIGNATURE)
UPIPE_HELPER_UREFCOUNT(upipe_rtp_vc2_unpack, urefcount, upipe_rtp_vc2_unpack_free)
UPIPE_HELPER_VOID(upipe_rtp_vc2_unpack)
UPIPE_HELPER_OUTPUT(upipe_rtp_vc2_unpack, output, flow_def, output_state,
                    request_list)
UPIPE_HELPER_UBUF_MGR(upipe_rtp_vc2_unpack, ubuf_mgr, flow_format, ubuf_mgr_request,
                      upipe_rtp_vc2_unpack_check,
                      upipe_rtp_vc2_unpack_register_output_request,
                      upipe_rtp_vc2_unpack_unregister_output_request)
UPIPE_HELPER_INPUT(upipe_rtp_vc2_unpack, urefs, nb_urefs, max_urefs, blockers,
        upipe_rtp_vc2_unpack_handle)

static int upipe_rtp_vc2_unpack_check(struct upipe *upipe, struct uref *flow_format)
{
    if (flow_format)
        upipe_rtp_vc2_unpack_store_flow_def(upipe, flow_format);

    bool was_buffered = !upipe_rtp_vc2_unpack_check_input(upipe);
    upipe_rtp_vc2_unpack_output_input(upipe);
    upipe_rtp_vc2_unpack_unblock_input(upipe);
    if (was_buffered && upipe_rtp_vc2_unpack_check_input(upipe)) {
        /* All unpackets have been output, release again the pipe that has been
         * used in @ref upipe_rtp_vc2_unpack_input. */
        upipe_release(upipe);
    }

    return UBASE_ERR_NONE;
}

static int upipe_rtp_vc2_unpack_set_flow_def(struct upipe *upipe,
                                       struct uref *flow_def)
{
    struct upipe_rtp_vc2_unpack *upipe_rtp_vc2_unpack = upipe_rtp_vc2_unpack_from_upipe(upipe);
    if (flow_def == NULL)
        return UBASE_ERR_INVALID;

    UBASE_RETURN(uref_flow_match_def(flow_def, "block."))

    struct uref *flow_def_dup = uref_dup(flow_def);
    if (unlikely(flow_def_dup == NULL)) {
        upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
        return UBASE_ERR_ALLOC;
    }

    uref_flow_set_def(flow_def_dup, "block.dirac.pic.");

    upipe_rtp_vc2_unpack_require_ubuf_mgr(upipe, flow_def_dup);

    return UBASE_ERR_NONE;
}

static int upipe_rtp_vc2_unpack_control(struct upipe *upipe, int command,
                                  va_list args)
{
    switch (command) {
        case UPIPE_REGISTER_REQUEST: {
            struct urequest *request = va_arg(args, struct urequest *);
            if (request->type == UREQUEST_FLOW_FORMAT ||
                request->type == UREQUEST_UBUF_MGR)
                return upipe_throw_provide_request(upipe, request);
            return upipe_rtp_vc2_unpack_alloc_output_proxy(upipe, request);
        }
        case UPIPE_UNREGISTER_REQUEST: {
            struct urequest *request = va_arg(args, struct urequest *);
            if (request->type == UREQUEST_FLOW_FORMAT ||
                request->type == UREQUEST_UBUF_MGR)
                return UBASE_ERR_NONE;
            return upipe_rtp_vc2_unpack_free_output_proxy(upipe, request);
        }
        case UPIPE_SET_FLOW_DEF: {
            struct uref *flow_def = va_arg(args, struct uref *);
            return upipe_rtp_vc2_unpack_set_flow_def(upipe, flow_def);
        }
        case UPIPE_GET_FLOW_DEF:
        case UPIPE_GET_OUTPUT:
        case UPIPE_SET_OUTPUT:
            return upipe_rtp_vc2_unpack_control_output(upipe, command, args);
        default:
            return UBASE_ERR_UNHANDLED;
    }
}

static void upipe_rtp_vc2_unpack_free(struct upipe *upipe)
{
    upipe_rtp_vc2_unpack_clean_output(upipe);
    upipe_rtp_vc2_unpack_clean_urefcount(upipe);
    upipe_rtp_vc2_unpack_clean_ubuf_mgr(upipe);
    upipe_rtp_vc2_unpack_clean_input(upipe);
    upipe_rtp_vc2_unpack_free_void(upipe);
}

static struct upipe *upipe_rtp_vc2_unpack_alloc(struct upipe_mgr *mgr,
                                           struct uprobe *uprobe,
                                           uint32_t signature,
                                           va_list args)
{
    struct upipe *upipe =
        upipe_rtp_vc2_unpack_alloc_void(mgr, uprobe, signature, args);
    if (unlikely(upipe == NULL))
        return NULL;
    struct upipe_rtp_vc2_unpack *upipe_rtp_vc2_unpack = upipe_rtp_vc2_unpack_from_upipe(upipe);

    upipe_rtp_vc2_unpack_init_urefcount(upipe);
    upipe_rtp_vc2_unpack_init_ubuf_mgr(upipe);
    upipe_rtp_vc2_unpack_init_input(upipe);
    upipe_rtp_vc2_unpack_init_output(upipe);

    return upipe;
}

static void upipe_rtp_vc2_unpack_input(struct upipe *upipe, struct uref *uref,
                                  struct upump **upump_p)
{
    if (!upipe_rtp_vc2_unpack_check_input(upipe)) {
        upipe_rtp_vc2_unpack_hold_input(upipe, uref);
        upipe_rtp_vc2_unpack_block_input(upipe, upump_p);
    } else if (!upipe_rtp_vc2_unpack_handle(upipe, uref, upump_p)) {
        upipe_rtp_vc2_unpack_hold_input(upipe, uref);
        upipe_rtp_vc2_unpack_block_input(upipe, upump_p);
        /* Increment upipe refcount to avoid disappearing before all unpackets
         * have been sent. */
        upipe_use(upipe);
    }
}

static void write_parse_info(struct upipe_rtp_vc2_unpack *ctx, uint8_t *dst,
        uint8_t parse_code, uint32_t next_offset)
{
    dst[0] = 'B';
    dst[1] = 'B';
    dst[2] = 'C';
    dst[3] = 'D';
    dst[4] = parse_code;
    AV_WB32(dst + 5, next_offset);
    AV_WB32(dst + 9, ctx->prev_offset);
    ctx->prev_offset = next_offset;
}

static bool upipe_rtp_vc2_unpack_handle(struct upipe *upipe, struct uref *uref,
                                  struct upump **upump_p)
{
    struct upipe_rtp_vc2_unpack *ctx = upipe_rtp_vc2_unpack_from_upipe(upipe);
    if (!ctx->ubuf_mgr)
        return false;

    const uint8_t *src = NULL;
    int src_size = -1;
    int err = uref_block_read(uref, 0, &src_size, &src);
    if (!ubase_check(err)) {
        upipe_throw_fatal(upipe, err);
        uref_free(uref);
        return true;
    }

    /* NOTE: assuming RTP headers have not been removed. */

    uint8_t parse_code = src[RTP_HEADER_SIZE + 3];

    if (parse_code == DIRAC_PCODE_SEQ_HEADER) {
        upipe_dbg(upipe, "found sequence header");

        size_t data_unit_size = PARSE_INFO_HEADER_SIZE
                              + src_size
                              - 4 /* ext seqnum, reserved byte, parse code */
                              - RTP_HEADER_SIZE;
        /* TODO: check size is less than INT_MAX */

        struct ubuf *data_unit = ubuf_block_alloc(ctx->ubuf_mgr, data_unit_size);
        if (!data_unit) {
            upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
            goto no_output;
        }

        uint8_t *dst = NULL;
        int dst_size = -1;
        UBASE_RETURN(ubuf_block_write(data_unit, 0, &dst_size, &dst));
        /* TODO: check dst_size is equal to data_unit_size */

        write_parse_info(ctx, dst, DIRAC_PCODE_SEQ_HEADER, data_unit_size);
        memcpy(dst + PARSE_INFO_HEADER_SIZE,
                src + RTP_HEADER_SIZE + 4,
                src_size - 4);

        ubuf_block_unmap(data_unit, 0);
        uref_attach_ubuf(uref, data_unit);
        upipe_rtp_vc2_unpack_output(upipe, uref, upump_p);
    }

    else if (parse_code == DIRAC_PCODE_AUX) {
        upipe_dbg(upipe, "found auxiliary data, skipping");
        goto no_output;
    }

    else if (parse_code == DIRAC_PCODE_PICTURE_FRAGMENT_HQ) {
        upipe_dbg(upipe, "found HQ picture fragment");

        uint32_t picture_number = AV_RB32(src + RTP_HEADER_SIZE + 4);
        uint16_t slice_count = AV_RB16(src + RTP_HEADER_SIZE + 14);
        size_t fragment_data_length = src_size
                                    - 4 /* ext seqnum, reserved byte, parse code */
                                    - 4 /* picture number */
                                    - 4 /* slice prefix bytes, slice size scaler */
                                    - 4 /* fragment length, slice count */
                                    - RTP_HEADER_SIZE;
        /* TODO: check fragment_data_length is less than UINT16_MAX */

        /* When slice_count is not zero the fragment_data_length will include
         * the 4 bytes for fragment x and y offsets. */

        size_t data_unit_size = PARSE_INFO_HEADER_SIZE
                              + 8 /* picture number, data length, slice count */
                              + fragment_data_length;
        /* TODO: check size is less than INT_MAX */

        struct ubuf *data_unit = ubuf_block_alloc(ctx->ubuf_mgr, data_unit_size);
        if (!data_unit) {
            upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
            goto no_output;
        }

        uint8_t *dst = NULL;
        int dst_size = -1;
        UBASE_RETURN(ubuf_block_write(data_unit, 0, &dst_size, &dst));
        /* TODO: check dst_size is equal to data_unit_size */

        write_parse_info(ctx, dst, DIRAC_PCODE_PICTURE_FRAGMENT_HQ, data_unit_size);
        AV_WB32(dst + PARSE_INFO_HEADER_SIZE, picture_number);

        if (slice_count)
            AV_WB16(dst + PARSE_INFO_HEADER_SIZE + 4, fragment_data_length - 4); /* The fragment x/y offsets should not be included. */
        else
            AV_WB16(dst + PARSE_INFO_HEADER_SIZE + 4, fragment_data_length);

        AV_WB16(dst + PARSE_INFO_HEADER_SIZE + 6, slice_count);
        memcpy(dst + PARSE_INFO_HEADER_SIZE + 8,
                src + RTP_HEADER_SIZE + 16,
                fragment_data_length); /* includes fragment x/y offset when slice_count is not zero. */

        ubuf_block_unmap(data_unit, 0);
        uref_attach_ubuf(uref, data_unit);
        upipe_rtp_vc2_unpack_output(upipe, uref, upump_p);
    }

    else if (parse_code == DIRAC_PCODE_END_SEQ) {
        upipe_dbg(upipe, "found end sequence");

        struct ubuf *data_unit = ubuf_block_alloc(ctx->ubuf_mgr, PARSE_INFO_HEADER_SIZE);
        if (!data_unit) {
            upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
            goto no_output;
        }

        uint8_t *dst = NULL;
        int dst_size = -1;
        UBASE_RETURN(ubuf_block_write(data_unit, 0, &dst_size, &dst));
        /* TODO: check dst_size is equal to data_unit_size */

        write_parse_info(ctx, dst, DIRAC_PCODE_END_SEQ, PARSE_INFO_HEADER_SIZE);

        ubuf_block_unmap(data_unit, 0);
        uref_attach_ubuf(uref, data_unit);
        upipe_rtp_vc2_unpack_output(upipe, uref, upump_p);
    }

    else if (parse_code == DIRAC_PCODE_PAD) {
        upipe_dbg(upipe, "found padding, skipping");
        goto no_output;
    }

    else {
        upipe_err_va(upipe, "unknown parse code (0x%x)", (int)parse_code);
        upipe_throw_fatal(upipe, UBASE_ERR_LOCAL);
        goto no_output;
    }

    uref_block_unmap(uref, 0);
    return true;

no_output:
    uref_block_unmap(uref, 0);
    uref_free(uref);
    return true;
}

static struct upipe_mgr upipe_rtp_vc2_unpack_mgr = {
    .refcount = NULL,
    .signature = UPIPE_RTP_VC2_UNPACK_SIGNATURE,

    .upipe_alloc = upipe_rtp_vc2_unpack_alloc,
    .upipe_input = upipe_rtp_vc2_unpack_input,
    .upipe_control = upipe_rtp_vc2_unpack_control,

    .upipe_mgr_control = NULL,
};

struct upipe_mgr *upipe_rtp_vc2_unpack_mgr_alloc(void)
{
    return &upipe_rtp_vc2_unpack_mgr;
}
