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
 * @short Upipe module to split vc2 bitstream into RTP packets
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
#include <upipe/upipe_helper_uref_stream.h>
#include <upipe/upipe_helper_input.h>

#include <bitstream/ietf/rtp.h>
#include <libavutil/intreadwrite.h>

#include <upipe-modules/upipe_rtp_vc2_pack.h>

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

struct upipe_rtp_vc2_pack {
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

    /** next uref to be processed */
    struct uref *next_uref;
    /** original size of the next uref */
    size_t next_uref_size;
    /** urefs received after next uref */
    struct uchain urefs;

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

    uint64_t sequence_number;

    uint32_t major_version, picture_coding_mode, slices_x;
    uint16_t slice_prefix_bytes, slice_size_scaler;
};

struct state {
    const uint8_t *current_byte;
    int next_bit;
};

static inline void init_state(struct state *state, const uint8_t *ptr)
{
    state->current_byte = ptr;
    state->next_bit = 7;
}

static inline void read_byte(struct state *state)
{
    state->next_bit = 7;
    state->current_byte += 1;
}

static inline uint32_t read_bit(struct state *state)
{
    uint32_t bit = (*state->current_byte >> state->next_bit) & 1;
    state->next_bit -= 1;
    if (state->next_bit < 0) {
        read_byte(state);
    }
    return bit;
}

static inline bool read_bool(struct state *state)
{
    if (read_bit(state) == 1)
        return true;
    else
        return false;
}

static inline uint32_t read_uint(struct state *state)
{
    uint32_t value = 1;
    while (read_bit(state) == 0) {
        value <<= 1;
        if (read_bit(state) == 1) {
            value += 1;
        }
    }
    value -= 1;
    return value;
}

static inline void skip_bit(struct state *state)
{
    state->next_bit -= 1;
    if (state->next_bit < 0)
        read_byte(state);
}

static inline void skip_uint(struct state *state)
{
    while (read_bit(state) == 0)
        skip_bit(state);
}

static void parse_sequence_header(struct upipe_rtp_vc2_pack *ctx, const uint8_t *ptr)
{
    struct state state;
    init_state(&state, ptr);
    ctx->major_version = read_uint(&state);
    skip_uint(&state); /* minor version */
    skip_uint(&state); /* profile */
    skip_uint(&state); /* level */
    skip_uint(&state); /* base video format */
    if (read_bool(&state)) { /* custom dimensions flag */
        skip_uint(&state); /* frame width */
        skip_uint(&state); /* frame height */
    }
    if (read_bool(&state)) /* custom color difference format flag */
        skip_uint(&state); /* color difference format index */
    if (read_bool(&state)) /* custom scan format flag */
        skip_uint(&state); /* source sampling */
    if (read_bool(&state)) /* custom frame rate flag */
        if (read_uint(&state) == 0) { /* frame rate index */
            skip_uint(&state); /* frame rate numerator */
            skip_uint(&state); /* frame rate denominator */
        }
    if (read_bool(&state)) /* custom pixel aspect ratio flag */
        if (read_uint(&state) == 0) { /* pixel aspect ratio index */
            skip_uint(&state); /* pixel aspect ratio numerator */
            skip_uint(&state); /* pixel aspect ratio denominator */
        }
    if (read_bool(&state)) { /* custom clean area flag */
        skip_uint(&state); /* clean width */
        skip_uint(&state); /* clean height */
        skip_uint(&state); /* left offset */
        skip_uint(&state); /* top offset */
    }
    if (read_bool(&state)) /*custom signal range flag */
        if (read_uint(&state) == 0) { /* signal range index */
            skip_uint(&state); /* luma offset */
            skip_uint(&state); /* luma excursion */
            skip_uint(&state); /* color diff offset */
            skip_uint(&state); /* color diff excursion */
        }
    if (read_bool(&state)) /* custom color spec flag */
        if (read_uint(&state) == 0) { /*color spec index */
            if (read_bool(&state)) /* custom color primaries flag */
                skip_uint(&state);
            if (read_bool(&state)) /* custom color matrix flag */
                skip_uint(&state);
            if (read_bool(&state)) /* custom transfer function flag */
                skip_uint(&state);
        }
    ctx->picture_coding_mode = read_uint(&state);
}

static void parse_transform_paramters(struct upipe_rtp_vc2_pack *ctx, const uint8_t *ptr)
{
    struct state state;
    init_state(&state, ptr);
    skip_uint(&state); /* wavelet index */
    skip_uint(&state); /* dwt depth */
    if (ctx->major_version >= 3) {
        if (read_bool(&state)) /* asymmetric transform index flag */
            skip_uint(&state); /* wavelet index horizontal */
        if (read_bool(&state)) /* asymmetric transform flag */
            skip_uint(&state); /* dwt depth horizontal */
    }
    ctx->slices_x = read_uint(&state);
    skip_uint(&state); /* num slices y */
    /* RTP VC2 is only defined for HQ pictures */
    ctx->slice_prefix_bytes = read_uint(&state);
    ctx->slice_size_scaler = read_uint(&state);
}

/** @hidden */
static int upipe_rtp_vc2_pack_check(struct upipe *upipe, struct uref *flow_format);

/** @hidden */
static bool upipe_rtp_vc2_pack_handle(struct upipe *upipe, struct uref *uref,
                                  struct upump **upump_p);

UPIPE_HELPER_UPIPE(upipe_rtp_vc2_pack, upipe, UPIPE_RTP_VC2_PACK_SIGNATURE)
UPIPE_HELPER_UREFCOUNT(upipe_rtp_vc2_pack, urefcount, upipe_rtp_vc2_pack_free)
UPIPE_HELPER_VOID(upipe_rtp_vc2_pack)
UPIPE_HELPER_OUTPUT(upipe_rtp_vc2_pack, output, flow_def, output_state,
                    request_list)
UPIPE_HELPER_UBUF_MGR(upipe_rtp_vc2_pack, ubuf_mgr, flow_format, ubuf_mgr_request,
                      upipe_rtp_vc2_pack_check,
                      upipe_rtp_vc2_pack_register_output_request,
                      upipe_rtp_vc2_pack_unregister_output_request)
UPIPE_HELPER_UREF_STREAM(upipe_rtp_vc2_pack, next_uref, next_uref_size, urefs, NULL)
UPIPE_HELPER_INPUT(upipe_rtp_vc2_pack, input_urefs, nb_urefs, max_urefs, blockers,
        upipe_rtp_vc2_pack_handle)

static int upipe_rtp_vc2_pack_check(struct upipe *upipe, struct uref *flow_format)
{
    if (flow_format)
        upipe_rtp_vc2_pack_store_flow_def(upipe, flow_format);

    bool was_buffered = !upipe_rtp_vc2_pack_check_input(upipe);
    upipe_rtp_vc2_pack_output_input(upipe);
    upipe_rtp_vc2_pack_unblock_input(upipe);
    if (was_buffered && upipe_rtp_vc2_pack_check_input(upipe)) {
        /* All packets have been output, release again the pipe that has been
         * used in @ref upipe_rtp_vc2_pack_input. */
        upipe_release(upipe);
    }

    return UBASE_ERR_NONE;
}

static int upipe_rtp_vc2_pack_set_flow_def(struct upipe *upipe,
                                       struct uref *flow_def)
{
    struct upipe_rtp_vc2_pack *upipe_rtp_vc2_pack = upipe_rtp_vc2_pack_from_upipe(upipe);

    if (flow_def == NULL)
        return UBASE_ERR_INVALID;
    UBASE_RETURN(uref_flow_match_def(flow_def, "block.dirac.pic."))

    struct uref *flow_def_dup = uref_sibling_alloc(flow_def);
    if (unlikely(flow_def_dup == NULL)) {
        upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
        return UBASE_ERR_ALLOC;
    }

    uref_flow_set_def(flow_def_dup, "block.rtp.vc2.");

    upipe_rtp_vc2_pack_require_ubuf_mgr(upipe, flow_def_dup);

    return UBASE_ERR_NONE;
}

static int upipe_rtp_vc2_pack_control(struct upipe *upipe, int command,
                                  va_list args)
{
    switch (command) {
        case UPIPE_REGISTER_REQUEST: {
            struct urequest *request = va_arg(args, struct urequest *);
            if (request->type == UREQUEST_FLOW_FORMAT ||
                request->type == UREQUEST_UBUF_MGR)
                return upipe_throw_provide_request(upipe, request);
            return upipe_rtp_vc2_pack_alloc_output_proxy(upipe, request);
        }
        case UPIPE_UNREGISTER_REQUEST: {
            struct urequest *request = va_arg(args, struct urequest *);
            if (request->type == UREQUEST_FLOW_FORMAT ||
                request->type == UREQUEST_UBUF_MGR)
                return UBASE_ERR_NONE;
            return upipe_rtp_vc2_pack_free_output_proxy(upipe, request);
        }
        case UPIPE_SET_FLOW_DEF: {
            struct uref *flow_def = va_arg(args, struct uref *);
            return upipe_rtp_vc2_pack_set_flow_def(upipe, flow_def);
        }
        case UPIPE_GET_FLOW_DEF:
        case UPIPE_GET_OUTPUT:
        case UPIPE_SET_OUTPUT:
            return upipe_rtp_vc2_pack_control_output(upipe, command, args);
        default:
            return UBASE_ERR_UNHANDLED;
    }
}

static void upipe_rtp_vc2_pack_free(struct upipe *upipe)
{
    upipe_rtp_vc2_pack_clean_ubuf_mgr(upipe);
    upipe_rtp_vc2_pack_clean_urefcount(upipe);
    upipe_rtp_vc2_pack_clean_uref_stream(upipe);
    upipe_rtp_vc2_pack_clean_output(upipe);
    upipe_rtp_vc2_pack_clean_input(upipe);
    upipe_rtp_vc2_pack_free_void(upipe);
}

static struct upipe *upipe_rtp_vc2_pack_alloc(struct upipe_mgr *mgr,
                                           struct uprobe *uprobe,
                                           uint32_t signature,
                                           va_list args)
{
    struct upipe *upipe =
        upipe_rtp_vc2_pack_alloc_void(mgr, uprobe, signature, args);
    if (unlikely(upipe == NULL))
        return NULL;
    struct upipe_rtp_vc2_pack *upipe_rtp_vc2_pack = upipe_rtp_vc2_pack_from_upipe(upipe);

    upipe_rtp_vc2_pack_init_urefcount(upipe);
    upipe_rtp_vc2_pack_init_input(upipe);
    upipe_rtp_vc2_pack_init_ubuf_mgr(upipe);
    upipe_rtp_vc2_pack_init_uref_stream(upipe);
    upipe_rtp_vc2_pack_init_output(upipe);

    return upipe;
}

static void upipe_rtp_vc2_pack_input(struct upipe *upipe, struct uref *uref,
                                  struct upump **upump_p)
{
    if (!upipe_rtp_vc2_pack_check_input(upipe)) {
        upipe_rtp_vc2_pack_hold_input(upipe, uref);
        upipe_rtp_vc2_pack_block_input(upipe, upump_p);
    } else if (!upipe_rtp_vc2_pack_handle(upipe, uref, upump_p)) {
        upipe_rtp_vc2_pack_hold_input(upipe, uref);
        upipe_rtp_vc2_pack_block_input(upipe, upump_p);
        /* Increment upipe refcount to avoid disappearing before all packets
         * have been sent. */
        upipe_use(upipe);
    }
}

#define MTU 1440 /* MTU from rtp pcm pack */

static int output_packet(struct upipe *upipe, struct uref *uref, struct upump **upump_p,
        struct ubuf *packet, uint8_t parse_code, uint64_t clock)
{
    struct upipe_rtp_vc2_pack *rtp_vc2_pack = upipe_rtp_vc2_pack_from_upipe(upipe);
    uint8_t *dst = NULL;
    int dst_size = -1;
    UBASE_RETURN(ubuf_block_write(packet, 0, &dst_size, &dst));

    memset(dst, 0, RTP_HEADER_SIZE);
    rtp_set_hdr(dst);

    /* sequence number */
    rtp_set_seqnum(dst, rtp_vc2_pack->sequence_number);
    /* extended sequence number */
    dst[RTP_HEADER_SIZE + 0] = (rtp_vc2_pack->sequence_number >> 16);
    dst[RTP_HEADER_SIZE + 1] = (rtp_vc2_pack->sequence_number >> 24);
    rtp_vc2_pack->sequence_number++;

    /* parse code */
    dst[RTP_HEADER_SIZE + 3] = parse_code;

    ubuf_block_unmap(packet, 0);
    struct uref *uref_packet = uref_fork(uref, packet);
    if (unlikely(!uref_packet))
        return UBASE_ERR_ALLOC;

    clock -= UCLOCK_FREQ / 25; // FIXME 16:35 < Meuuh> the last packet should have cr_sys = dts_sys
    uref_clock_set_cr_sys(uref_packet, clock);

    /*TODO: get errors from output? */
    upipe_rtp_vc2_pack_output(upipe, uref_packet, upump_p);
    return UBASE_ERR_NONE;
}

static bool upipe_rtp_vc2_pack_handle(struct upipe *upipe, struct uref *uref,
                                  struct upump **upump_p)
{
    struct upipe_rtp_vc2_pack *rtp_vc2_pack = upipe_rtp_vc2_pack_from_upipe(upipe);
    if (!rtp_vc2_pack->ubuf_mgr)
        return false;

    const uint8_t *src = NULL;
    int src_size = -1;
    int err = uref_block_read(uref, 0, &src_size, &src);
    if (unlikely(!ubase_check(err))) {
        upipe_throw_fatal(upipe, err);
        uref_free(uref);
        return true;
    }

    //copy data from input to output

    /* Put sequence header into first RTP packet:
     * - remove parse info header
     *
     * Put transform parameters into second RTP packet:
     * - remove parse info header
     * - remove fragment header
     * - keep picture number
     * - keep slice prefix bytes
     * - keep slice scaler
     * - keep/create fragment length
     *
     * Put slices into subsequent RTP packets:
     * - remove parse info header
     * - remove fragment header
     * - keep picture number
     * - keep slice prefix bytes
     * - keep slice scaler
     * - keep/create fragment length
     * - keep/create slice x/y offsets
     */

    /* About timing.
     * k.sys and k.prog start with the same value on the first uref made by the
     * rowsplit module.  That increments k.sys for each subsequent uref it
     * outputs based on its position down the frame.
     *
     * k.duration remains the same for each uref: the duration of one whole
     * frame.
     *
     * To space packets across 1 row I should read the fraction_duration
     * attribute from the uref (which I added in rowsplit).  Then I should add
     * more to k.sys based on the position a slice has across the row.
     */

    uint64_t clock;
    err = uref_clock_get_dts_sys(uref, &clock);
    if (unlikely(!ubase_check(err))) {
        upipe_err(upipe, "unable to get clock");
        upipe_throw_fatal(upipe, err);
        goto end;
    }

    uint64_t fraction_duration;
    err = uref_attr_get_unsigned(uref, &fraction_duration, UDICT_TYPE_UNSIGNED, "fraction_duration");
    if (unlikely(!ubase_check(err))) {
        upipe_warn(upipe, "unable to get fraction_duration");
        fraction_duration = 0;
    }

    ptrdiff_t src_offset = 0;
    /* some loop over parse info blocks in the input buffer */
    do {
        if (AV_RN32(src + src_offset) != UBASE_FOURCC('B', 'B', 'C', 'D')) {
            upipe_err(upipe, "incorrect magic prefix");
            upipe_throw_fatal(upipe, UBASE_ERR_UNKNOWN);
            goto end;
        }
        uint8_t parse_code = src[src_offset + 4];
        ptrdiff_t next_offset = AV_RB32(src + src_offset + 5);
        ptrdiff_t prev_offset = AV_RB32(src + src_offset + 9);

        if (next_offset && parse_code == DIRAC_PCODE_END_SEQ)
            next_offset = PARSE_INFO_HEADER_SIZE;

        else if (!next_offset) {
            upipe_err(upipe, "next offset is 0, this is not currently supported");
            upipe_throw_fatal(upipe, UBASE_ERR_UNKNOWN);
            goto end;
        }

        else if (next_offset >= src_size) {
            upipe_err(upipe, "next offset is greater than the size of the input uref, dropping");
            upipe_throw_fatal(upipe, UBASE_ERR_UNKNOWN);
            goto end;
        }

        else if (next_offset < PARSE_INFO_HEADER_SIZE) {
            upipe_warn(upipe, "next offset is smaller than PARSE_INFO_HEADER_SIZE, skipping");
            goto skip;
        }

        if (parse_code == DIRAC_PCODE_SEQ_HEADER) {
            upipe_dbg(upipe, "found sequence header");

            if (next_offset <= PARSE_INFO_HEADER_SIZE) {
                upipe_warn(upipe, "next offset too small for DIRAC_PCODE_SEQ_HEADER, skipping");
                goto skip;
            }

            size_t packet_size = RTP_HEADER_SIZE
                               + 4 /* ext seqnum, reserved byte, parse code */
                               + next_offset
                               - PARSE_INFO_HEADER_SIZE;
            /* TODO: check size is less than INT_MAX */

            struct ubuf *packet = ubuf_block_alloc(rtp_vc2_pack->ubuf_mgr, packet_size);
            if (unlikely(!packet)) {
                upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
                goto end;
            }

            uint8_t *dst = NULL;
            int dst_size = -1;
            err = ubuf_block_write(packet, 0, &dst_size, &dst);
            if (unlikely(!ubase_check(err))) {
                upipe_throw_fatal(upipe, err);
                ubuf_free(packet);
                goto end;
            }
            /* TODO: check dst_size is equal to packet_size */

            parse_sequence_header(rtp_vc2_pack, src + src_offset + PARSE_INFO_HEADER_SIZE);

            memcpy(dst + RTP_HEADER_SIZE + 4,
                    src + src_offset + PARSE_INFO_HEADER_SIZE,
                    next_offset - PARSE_INFO_HEADER_SIZE);

            ubuf_block_unmap(packet, 0);
            err = output_packet(upipe, uref, upump_p, packet, parse_code,
                    clock + src_offset * fraction_duration /src_size);
            if (unlikely(!ubase_check(err))) {
                upipe_throw_fatal(upipe, err);
                ubuf_free(packet);
                goto end;
            }
        }

        else if (parse_code == DIRAC_PCODE_AUX) {
            upipe_dbg(upipe, "found auxiliary data, skipping");
        }

        else if (parse_code == DIRAC_PCODE_PICTURE_HQ) {
            upipe_dbg(upipe, "found HQ picture");
            upipe_err(upipe, "splitting HQ pictures into fragments and "
                    "packets is not implemented at this time, skipping picture");
        }

        else if (parse_code == DIRAC_PCODE_PICTURE_FRAGMENT_HQ) {
            /* check that there is enough input data for the parse info header,
             * the fragment header, and 1 byte of data */
            if (next_offset < PARSE_INFO_HEADER_SIZE + 8 + 1) {
                upipe_warn(upipe, "next offset too small for DIRAC_PCODE_PICTURE_FRAGMENT_HQ, skipping");
                goto skip;
            }

            uint32_t picture_number = AV_RB32(src + src_offset + 13);
            //uint16_t fragment_data_length = AV_RB16(src + src_offset + 17);
            uint16_t fragment_slice_count = AV_RB16(src + src_offset + 19);
            uint16_t fragment_x_offset = 0;

            /* TODO: check size is less than INT_MAX */
            size_t packet_size = RTP_HEADER_SIZE
                               + 4 /* ext seqnum, reserved byte, parse code */
                               + 4 /* picture number */
                               + 4 /* slice prefix bytes, slice size scaler */
                               + 4 /* fragment length, num slices */
                               + next_offset
                               - PARSE_INFO_HEADER_SIZE;
            if (fragment_slice_count) {
                packet_size += 4; /* slice x/y offset */
                fragment_x_offset = AV_RB16(src + src_offset + 21);
            } else {
                parse_transform_paramters(rtp_vc2_pack, src + src_offset + PARSE_INFO_HEADER_SIZE + 8);
            }

            struct ubuf *packet = ubuf_block_alloc(rtp_vc2_pack->ubuf_mgr, packet_size);
            if (unlikely(!packet)) {
                upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
                goto end;
            }

            uint8_t *dst = NULL;
            int dst_size = -1;
            err = ubuf_block_write(packet, 0, &dst_size, &dst);
            if (unlikely(!ubase_check(err))) {
                upipe_throw_fatal(upipe, err);
                ubuf_free(packet);
                goto end;
            }
            /* TODO: check dst_size is equal to packet_size */

            int field = 0;
            if (rtp_vc2_pack->picture_coding_mode == 1)
                field = 1 + (picture_number & 1);
            dst[RTP_HEADER_SIZE + 3] |= (field != 0) << 6; /* picture is interlaced */
            dst[RTP_HEADER_SIZE + 3] |= (field == 2) << 7; /* second field */

            AV_WB32(dst + RTP_HEADER_SIZE + 4, picture_number);
            AV_WB16(dst + RTP_HEADER_SIZE + 8, rtp_vc2_pack->slice_prefix_bytes);
            AV_WB16(dst + RTP_HEADER_SIZE + 10, rtp_vc2_pack->slice_size_scaler);
            /* The input data can be copied straight to the output packet
             * because the endianess is the same and after the picture number in
             * the fragment header of the input data the layout is the same as
             * in the output packet after the slice prefix bytes and the slice
             * size scaler. */
            memcpy(dst + RTP_HEADER_SIZE + 12,
                    src + src_offset + PARSE_INFO_HEADER_SIZE + 4,
                    next_offset - PARSE_INFO_HEADER_SIZE - 4);

            ubuf_block_unmap(packet, 0);
            err = output_packet(upipe, uref, upump_p, packet, parse_code,
                    clock + src_offset*fraction_duration/src_size);
            if (unlikely(!ubase_check(err))) {
                upipe_throw_fatal(upipe, err);
                ubuf_free(packet);
                goto end;
            }
        }

        else if (parse_code == DIRAC_PCODE_END_SEQ) {
            upipe_dbg(upipe, "found end sequence");
            size_t packet_size = RTP_HEADER_SIZE
                               + 4; /* ext seqnum, reserved byte, parse code */

            struct ubuf *packet = ubuf_block_alloc(rtp_vc2_pack->ubuf_mgr, packet_size);
            if (unlikely(!packet)) {
                uref_free(uref);
                upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
                return true;
            }

            ubuf_block_unmap(packet, 0);
            err = output_packet(upipe, uref, upump_p, packet, parse_code,
                    clock + src_offset*fraction_duration/src_size);
            if (unlikely(!ubase_check(err))) {
                upipe_throw_fatal(upipe, err);
                ubuf_free(packet);
                goto end;
            }
        }

        else {
            upipe_err(upipe, "unknown parse code");
            upipe_throw_fatal(upipe, UBASE_ERR_LOCAL);
            goto end;
        }

skip:
        src_offset += next_offset;
    } while (src_offset < src_size);

end:
    uref_block_unmap(uref, 0);
    uref_free(uref);

    return true;
}

static struct upipe_mgr upipe_rtp_vc2_pack_mgr = {
    .refcount = NULL,
    .signature = UPIPE_RTP_VC2_PACK_SIGNATURE,

    .upipe_alloc = upipe_rtp_vc2_pack_alloc,
    .upipe_input = upipe_rtp_vc2_pack_input,
    .upipe_control = upipe_rtp_vc2_pack_control,

    .upipe_mgr_control = NULL,
};

struct upipe_mgr *upipe_rtp_vc2_pack_mgr_alloc(void)
{
    return &upipe_rtp_vc2_pack_mgr;
}
