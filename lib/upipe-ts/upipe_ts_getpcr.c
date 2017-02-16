/*
 * Copyright (C) 2012-2015 OpenHeadend S.A.R.L.
 * Copyright (C) 2016 Open Broadcast Systems Ltd
 *
 * Authors: Christophe Massiot
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
 * @short Upipe module extracting PCR from TS packets
 */

#include <upipe/ubase.h>
#include <upipe/ulist.h>
#include <upipe/uprobe.h>
#include <upipe/uref.h>
#include <upipe/uref_flow.h>
#include <upipe/uref_block.h>
#include <upipe/uref_clock.h>
#include <upipe/ubuf.h>
#include <upipe/uclock.h>
#include <upipe/upipe.h>
#include <upipe/upipe_helper_upipe.h>
#include <upipe/upipe_helper_urefcount.h>
#include <upipe/upipe_helper_void.h>
#include <upipe/upipe_helper_output.h>
#include <upipe-ts/upipe_ts_getpcr.h>

#include <stdlib.h>
#include <limits.h>
#include <stdbool.h>
#include <stdarg.h>
#include <string.h>
#include <assert.h>

#include <bitstream/mpeg/ts.h>

/** we only accept TS packets */
#define EXPECTED_FLOW_DEF "block.mpegts."

/** @internal @This is the private context of a ts_getpcr pipe. */
struct upipe_ts_getpcr {
    /** refcount management structure */
    struct urefcount urefcount;

    /** pipe acting as output */
    struct upipe *output;
    /** output flow definition packet */
    struct uref *flow_def;
    /** output state */
    enum upipe_helper_output_state output_state;
    /** list of output requests */
    struct uchain request_list;

    /** requested PCR PID */
    uint16_t pcr_pid;

    /** number of PCRs on an unexpected PID */
    uint8_t new_pcr_pid_count;

    /** previous PCR value */
    uint64_t last_pcr;

    /** arrival time of previous PCR value */
    uint64_t last_sys;

    /** number of PCR wraparounds */
    uint64_t wraparounds;

    /** last continuity counter on PCR */
    unsigned pcr_cc;

    /** public upipe structure */
    struct upipe upipe;
};

UPIPE_HELPER_UPIPE(upipe_ts_getpcr, upipe, UPIPE_TS_GETPCR_SIGNATURE)
UPIPE_HELPER_UREFCOUNT(upipe_ts_getpcr, urefcount, upipe_ts_getpcr_free)
UPIPE_HELPER_VOID(upipe_ts_getpcr)
UPIPE_HELPER_OUTPUT(upipe_ts_getpcr, output, flow_def, output_state, request_list)

/** @internal @This allocates a ts_getpcr pipe.
 *
 * @param mgr common management structure
 * @param uprobe structure used to raise events
 * @param signature signature of the pipe allocator
 * @param args optional arguments
 * @return pointer to upipe or NULL in case of allocation error
 */
static struct upipe *upipe_ts_getpcr_alloc(struct upipe_mgr *mgr,
                                           struct uprobe *uprobe,
                                           uint32_t signature, va_list args)
{
    struct upipe *upipe = upipe_ts_getpcr_alloc_void(mgr, uprobe, signature,
                                                     args);
    if (unlikely(upipe == NULL))
        return NULL;

    struct upipe_ts_getpcr *upipe_ts_getpcr = upipe_ts_getpcr_from_upipe(upipe);
    upipe_ts_getpcr_init_urefcount(upipe);
    upipe_ts_getpcr_init_output(upipe);
    upipe_ts_getpcr->pcr_pid = 0xffff;
    upipe_ts_getpcr->new_pcr_pid_count = 0;
    upipe_ts_getpcr->last_pcr = UINT64_MAX;
    upipe_ts_getpcr->last_sys = UINT64_MAX;
    upipe_ts_getpcr->wraparounds = 0;
    upipe_ts_getpcr->pcr_cc = UINT_MAX;

    upipe_throw_ready(upipe);
    return upipe;
}

/** @internal @This parses the TS PCR of a packet.
 */
static uint64_t upipe_ts_getpcr_read_pcr(struct upipe *upipe, struct uref *uref,
    bool has_payload)
{
    /* Read adaptation field length */
    uint8_t af_length;
    if (unlikely(!ubase_check(uref_block_extract(uref, TS_HEADER_SIZE, 1, &af_length)))) {
        uref_free(uref);
        upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
        return UINT64_MAX;
    }

    if (unlikely((!has_payload && af_length != 183) ||
                (has_payload && af_length >= 183))) {
        upipe_warn_va(upipe, "invalid adaptation field received (length %d)", af_length);
        return UINT64_MAX;
    }

    if (!af_length)
        return UINT64_MAX;

    /* Read adaptation field */
    uint8_t af_header;
    if (unlikely(!ubase_check(uref_block_extract(uref, TS_HEADER_SIZE + 1, 1, &af_header)))) {
        uref_free(uref);
        upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
        return UINT64_MAX;
    }

    if (!tsaf_has_pcr(&af_header - 1 - TS_HEADER_SIZE))
        return UINT64_MAX;

    /* Read PCR */
    uint8_t buffer_af[TS_HEADER_SIZE_PCR - TS_HEADER_SIZE_AF];
    const uint8_t *pcr = uref_block_peek(uref, TS_HEADER_SIZE + 2,
            TS_HEADER_SIZE_PCR - TS_HEADER_SIZE_AF, buffer_af);
    if (unlikely(pcr == NULL)) {
        uref_free(uref);
        upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
        return UINT64_MAX;
    }

    /* Extract stream PCR */
    uint64_t pcr_orig = (tsaf_get_pcr(pcr - TS_HEADER_SIZE_AF) * 300 +
            tsaf_get_pcrext(pcr - TS_HEADER_SIZE_AF));
    pcr_orig *= UCLOCK_FREQ / 27000000;

    UBASE_FATAL(upipe, uref_block_peek_unmap(uref, 2, buffer_af, pcr))

    return pcr_orig;
}

/** @internal @This parses the TS header of a packet.
 *
 * @param upipe description structure of the pipe
 * @param uref uref structure
 * @param upump_p reference to pump that generated the buffer
 */
static void upipe_ts_getpcr_input(struct upipe *upipe, struct uref *uref,
                                  struct upump **upump_p)
{
    struct upipe_ts_getpcr *upipe_ts_getpcr = upipe_ts_getpcr_from_upipe(upipe);

    bool discontinuity = ubase_check(uref_flow_get_discontinuity(uref));

    if (upipe_ts_getpcr->new_pcr_pid_count > 20) {
        upipe_ts_getpcr->pcr_pid = 0xffff;
        discontinuity = 1;
    }

    /* packet real-time arrival */
#define MAX_IAT (UCLOCK_FREQ / 10) /* 100ms */
    uint64_t cr_sys = 0;
    UBASE_FATAL(upipe, uref_clock_get_cr_sys(uref, &cr_sys))
    if (likely(upipe_ts_getpcr->last_sys != UINT64_MAX)) {
        if (unlikely(cr_sys - upipe_ts_getpcr->last_sys > MAX_IAT)) {
            discontinuity = 1;
            upipe_dbg_va(upipe, "time discontinuity %" PRIu64 " %" PRIu64 "",
                    cr_sys, upipe_ts_getpcr->last_sys);
        }
    }
    upipe_ts_getpcr->last_sys = cr_sys;

    /* Read TS buffer */
    uint8_t buffer[TS_HEADER_SIZE];
    const uint8_t *ts_header = uref_block_peek(uref, 0, TS_HEADER_SIZE, buffer);
    if (unlikely(ts_header == NULL)) {
        uref_free(uref);
        upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
        return;
    }

    /* Parse flags */
    bool has_payload = ts_has_payload(ts_header);
    bool has_adaptation = ts_has_adaptation(ts_header);
    uint16_t pid = ts_get_pid(ts_header);
    uint8_t cc = ts_get_cc(ts_header);

    UBASE_FATAL(upipe, uref_block_peek_unmap(uref, 0, buffer, ts_header))

    /* Check continuity counter on PCR PID */
    if (pid == upipe_ts_getpcr->pcr_pid && has_payload) {
        if (unlikely(upipe_ts_getpcr->pcr_cc == UINT_MAX))
            upipe_ts_getpcr->pcr_cc = (cc + 16 - 1) & 0xf;

        if (cc != ((upipe_ts_getpcr->pcr_cc + 1) & 0xf)) {
            upipe_warn_va(upipe, "PCR discontinuity (cc %u -> %u)",
                    upipe_ts_getpcr->pcr_cc, cc);
            discontinuity = 1;
        }

        upipe_ts_getpcr->pcr_cc = cc;
    }

    /* No adaptation field = no PCR */
    if (!has_adaptation)
        goto end;

    /* Read PCR value */
    uint64_t pcr_orig = upipe_ts_getpcr_read_pcr(upipe, uref, has_payload);
    if (pcr_orig == UINT64_MAX)
        goto end;

    if (upipe_ts_getpcr->pcr_pid == 0xffff)
        upipe_ts_getpcr->pcr_pid = pid; /* we got a PCR PID */
    else if (upipe_ts_getpcr->pcr_pid != pid) {
        upipe_ts_getpcr->new_pcr_pid_count++; /* got a different PID */
        goto end;
    }

    /* Store original (stream) PCR */
    uref_clock_set_cr_orig(uref, pcr_orig);
    uref_clock_set_ref(uref);

    /* Check interval */
    if (upipe_ts_getpcr->last_pcr == UINT64_MAX)
        upipe_ts_getpcr->last_pcr = pcr_orig;
    uint64_t delta = pcr_orig + (1LLU << 33) - upipe_ts_getpcr->last_pcr;
    delta &= (1LLU << 33) - 1;
/** max interval between PCRs (ISO/IEC 13818-1 2.7.2) - could be 100 ms but
 * allow higher tolerance */
 #define MAX_PCR_INTERVAL (UCLOCK_FREQ / 2)
    if (delta > MAX_PCR_INTERVAL) {
        upipe_warn_va(upipe, "PCR interval too big");
        discontinuity = 1;
    }

    if (discontinuity)
        upipe_warn_va(upipe, "PCR discontinuity %" PRIu64 ": %"PRIu64 " -> %" PRIu64,
                delta, upipe_ts_getpcr->last_pcr, pcr_orig);

    if (pcr_orig < upipe_ts_getpcr->last_pcr)
        upipe_ts_getpcr->wraparounds++;

    upipe_ts_getpcr->last_pcr = pcr_orig;

    /* Make sure cr_prog increases monotonically */
    uint64_t prog = upipe_ts_getpcr->last_pcr + (upipe_ts_getpcr->wraparounds << 33);
    uref_clock_set_cr_prog(uref, prog);

    /* reset on discontinuities */
    if (discontinuity) {
        upipe_warn_va(upipe, "PCR RESET");
        upipe_ts_getpcr->new_pcr_pid_count = 0;
        upipe_ts_getpcr->wraparounds++;
        upipe_ts_getpcr->last_pcr = UINT64_MAX;
        upipe_ts_getpcr->pcr_cc = UINT_MAX;

        uref_flow_set_discontinuity(uref);
    }

    upipe_verbose_va(upipe, "PCR: %"PRId64" > %"PRId64", %"PRId64,
            pcr_orig, prog, delta);

    upipe_ts_getpcr->new_pcr_pid_count = 0;

end:
    upipe_ts_getpcr_output(upipe, uref, upump_p);
}

/** @internal @This sets the input flow definition.
 *
 * @param upipe description structure of the pipe
 * @param flow_def flow definition packet
 * @return an error code
 */
static int upipe_ts_getpcr_set_flow_def(struct upipe *upipe,
                                        struct uref *flow_def)
{
    if (flow_def == NULL)
        return UBASE_ERR_INVALID;

    const char *def;
    UBASE_RETURN(uref_flow_get_def(flow_def, &def))

    if (ubase_ncmp(def, EXPECTED_FLOW_DEF))
        return UBASE_ERR_INVALID;

    struct uref *flow_def_dup = uref_dup(flow_def);
    if (unlikely(flow_def_dup == NULL)) {
        upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
        return UBASE_ERR_ALLOC;
    }

    upipe_ts_getpcr_store_flow_def(upipe, flow_def_dup);
    return UBASE_ERR_NONE;
}

/** @internal @This processes control commands on a ts decaps pipe.
 *
 * @param upipe description structure of the pipe
 * @param command type of command to process
 * @param args arguments of the command
 * @return an error code
 */
static int upipe_ts_getpcr_control(struct upipe *upipe,
                                   int command, va_list args)
{
    switch (command) {
    case UPIPE_REGISTER_REQUEST: {
        struct urequest *request = va_arg(args, struct urequest *);
        return upipe_ts_getpcr_alloc_output_proxy(upipe, request);
    }
    case UPIPE_UNREGISTER_REQUEST: {
        struct urequest *request = va_arg(args, struct urequest *);
        return upipe_ts_getpcr_free_output_proxy(upipe, request);
    }
    case UPIPE_GET_FLOW_DEF: {
        struct uref **p = va_arg(args, struct uref **);
        return upipe_ts_getpcr_get_flow_def(upipe, p);
    }
    case UPIPE_SET_FLOW_DEF: {
        struct uref *flow_def = va_arg(args, struct uref *);
        return upipe_ts_getpcr_set_flow_def(upipe, flow_def);
    }
    case UPIPE_GET_OUTPUT: {
        struct upipe **p = va_arg(args, struct upipe **);
        return upipe_ts_getpcr_get_output(upipe, p);
    }
    case UPIPE_SET_OUTPUT: {
        struct upipe *output = va_arg(args, struct upipe *);
        return upipe_ts_getpcr_set_output(upipe, output);
    }
    default:
        return UBASE_ERR_UNHANDLED;
    }
}

/** @This frees a upipe.
 *
 * @param upipe description structure of the pipe
 */
static void upipe_ts_getpcr_free(struct upipe *upipe)
{
    upipe_throw_dead(upipe);

    struct upipe_ts_getpcr *upipe_ts_getpcr = upipe_ts_getpcr_from_upipe(upipe);
    upipe_ts_getpcr_clean_output(upipe);
    upipe_ts_getpcr_clean_urefcount(upipe);
    upipe_ts_getpcr_free_void(upipe);
}

/** module manager static descriptor */
static struct upipe_mgr upipe_ts_getpcr_mgr = {
    .refcount = NULL,
    .signature = UPIPE_TS_GETPCR_SIGNATURE,

    .upipe_alloc = upipe_ts_getpcr_alloc,
    .upipe_input = upipe_ts_getpcr_input,
    .upipe_control = upipe_ts_getpcr_control,

    .upipe_mgr_control = NULL
};

/** @This returns the management structure for all ts_getpcr pipes.
 *
 * @return pointer to manager
 */
struct upipe_mgr *upipe_ts_getpcr_mgr_alloc(void)
{
    return &upipe_ts_getpcr_mgr;
}
