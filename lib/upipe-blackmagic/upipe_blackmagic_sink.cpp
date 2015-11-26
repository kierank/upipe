/*
 * Copyright (C) 2014 Open Broadcast Systems Ltd.
 *
 * Authors: Kieran Kunhya
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston MA 02110-1301, USA.
 */

/** @file
 * @short Upipe bmd_sink module
 */

#define __STDC_LIMIT_MACROS    1
#define __STDC_FORMAT_MACROS   1
#define __STDC_CONSTANT_MACROS 1

#include <upipe/ubase.h>
#include <upipe/uatomic.h>
#include <upipe/ulist.h>
#include <upipe/uprobe.h>
#include <upipe/uclock.h>
#include <upipe/uref.h>
#include <upipe/uref_attr.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#   include <upipe/uref_block.h>
#pragma GCC diagnostic pop

#include <upipe/uref_block_flow.h>
#include <upipe/uref_pic.h>
#include <upipe/uref_pic_flow.h>
#include <upipe/uref_sound.h>
#include <upipe/uref_sound_flow.h>
#include <upipe/uref_clock.h>
#include <upipe/uref_dump.h>
#include <upipe/upipe.h>
#include <upipe/upipe_helper_input.h>
#include <upipe/upipe_helper_upipe.h>
#include <upipe/upipe_helper_upump_mgr.h>
#include <upipe/upipe_helper_upump.h>
#include <upipe/upipe_helper_urefcount.h>
#include <upipe/upipe_helper_void.h>
#include <upipe-blackmagic/upipe_blackmagic_sink.h>

#include <arpa/inet.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <assert.h>

#include <libavutil/intreadwrite.h>

#include "include/DeckLinkAPI.h"
#include "include/DeckLinkAPIDispatch.cpp"

#define CC_LINE 9

/* don't clip the v210 anc data */
#define WRITE_PIXELS(a, b, c)           \
    do {                                \
        val =  (*a);                    \
        val |= (*b << 10)  |            \
               (*c << 20);              \
        AV_WL32(dst, val);              \
        dst++;                          \
    } while (0)

class upipe_bmd_sink_frame : public IDeckLinkVideoFrame
{
public:
    upipe_bmd_sink_frame(struct uref *_uref, void *_buffer, long _width, long _height) :
                         uref(_uref), data(_buffer), width(_width), height(_height) {
        uatomic_store(&refcount, 1);
    }

    ~upipe_bmd_sink_frame(void) {
        uatomic_clean(&refcount);
        uref_pic_plane_unmap(uref, "u10y10v10y10u10y10v10y10u10y10v10y10", 0, 0, -1, -1);
        uref_free(uref);
    }

    virtual long STDMETHODCALLTYPE GetWidth(void) {
        return width;
    }

    virtual long STDMETHODCALLTYPE GetHeight(void) {
        return height;
    }

    virtual long STDMETHODCALLTYPE GetRowBytes(void) {
        return ((width + 47) / 48) * 128;
    }

    virtual BMDPixelFormat STDMETHODCALLTYPE GetPixelFormat(void) {
        return bmdFormat10BitYUV;
    }

    virtual BMDFrameFlags STDMETHODCALLTYPE GetFlags(void) {
        return bmdVideoOutputFlagDefault;
    }

    virtual HRESULT STDMETHODCALLTYPE GetBytes(void **buffer) {
        *buffer = data;
        return S_OK;
    }

    virtual HRESULT STDMETHODCALLTYPE GetTimecode(BMDTimecodeFormat format,
                                                  IDeckLinkTimecode **timecode) {
        *timecode = NULL;
        return S_FALSE;
    }

    virtual HRESULT STDMETHODCALLTYPE GetAncillaryData(IDeckLinkVideoFrameAncillary **ancillary) {
        *ancillary = frame_anc;
        return S_OK;
    }

    virtual HRESULT STDMETHODCALLTYPE SetAncillaryData(IDeckLinkVideoFrameAncillary *ancillary) {
        ancillary->AddRef();
        frame_anc = ancillary;
        return S_OK;
    }

    virtual ULONG STDMETHODCALLTYPE AddRef(void) {
        frame_anc->AddRef();
        return uatomic_fetch_add(&refcount, 1) + 1;
    }

    virtual ULONG STDMETHODCALLTYPE Release(void) {
        frame_anc->Release();
        uint32_t new_ref = uatomic_fetch_sub(&refcount, 1) - 1;
        if (new_ref == 0)
            delete this;
        return new_ref;
    }

    virtual HRESULT STDMETHODCALLTYPE QueryInterface(REFIID iid, LPVOID *ppv) {
        return E_NOINTERFACE;
    }

private:
    struct uref *uref;
    void *data;
    long width;
    long height;

    uatomic_uint32_t refcount;
    IDeckLinkVideoFrameAncillary *frame_anc;
};

/** @hidden */
static bool upipe_bmd_sink_sub_output(struct upipe *upipe, struct uref *uref,
                                      struct upump **upump_p);

/** @hidden */
static void upipe_bmd_sink_sub_write_watcher(struct upump *upump);

/** @internal @This is the private context of an output of an bmd_sink sink
 * pipe. */
struct upipe_bmd_sink_sub {
    struct upipe *upipe_bmd_sink;

    /** refcount management structure */
    struct urefcount urefcount;

    /** temporary uref storage */
    struct uchain urefs;
    /** nb urefs in storage */
    unsigned int nb_urefs;
    /** max urefs in storage */
    unsigned int max_urefs;
    /** list of blockers */
    struct uchain blockers;

    /** delay applied to pts attribute when uclock is provided */
    uint64_t latency;

    /** upump manager */
    struct upump_mgr *upump_mgr;
    /** watcher */
    struct upump *upump;

    /** public upipe structure */
    struct upipe upipe;
};

/** super-set of the uclock structure with additional local members */
struct uclock_bmd_sink {
    /** refcount management structure */
    struct urefcount urefcount;

    /** offset for discontinuities caused by format changes */
    uint64_t offset;

    /** structure exported to modules */
    struct uclock uclock;
};

UBASE_FROM_TO(uclock_bmd_sink, uclock, uclock, uclock)
UBASE_FROM_TO(uclock_bmd_sink, urefcount, urefcount, urefcount)

/** upipe_bmd_sink structure */
struct upipe_bmd_sink {
    /** refcount management structure */
    struct urefcount urefcount;

    /** manager to create subs */
    struct upipe_mgr sub_mgr;
    /** pic subpipe */
    struct upipe_bmd_sink_sub pic_subpipe;
    /** sound subpipe */
    struct upipe_bmd_sink_sub sound_subpipe;
    /** subpic subpipe */
    struct upipe_bmd_sink_sub subpic_subpipe;

    /** card index **/
    int card_idx;

    /** output mode **/
    char mode[5];

    /** started flag **/
    int started;

    /** vanc temporary buffer **/
    uint16_t vanc_tmp[1920*2];
    uint16_t *dc;

    /** closed captioning **/
    uint16_t cdp_hdr_sequence_cntr;

    /** handle to decklink card */
    IDeckLink *deckLink;
    /** handle to decklink card output */
    IDeckLinkOutput *deckLinkOutput;

    IDeckLinkDisplayMode *displayMode;

    /** hardware uclock */
    struct uclock_bmd_sink uclock;

    /** public upipe structure */
    struct upipe upipe;
};

UPIPE_HELPER_UPIPE(upipe_bmd_sink, upipe, UPIPE_BMD_SINK_SIGNATURE);
UPIPE_HELPER_UREFCOUNT(upipe_bmd_sink, urefcount, upipe_bmd_sink_free);
UPIPE_HELPER_VOID(upipe_bmd_sink);

UPIPE_HELPER_UPIPE(upipe_bmd_sink_sub, upipe, UPIPE_BMD_SINK_INPUT_SIGNATURE)
UPIPE_HELPER_UREFCOUNT(upipe_bmd_sink_sub, urefcount, upipe_bmd_sink_sub_free);
UPIPE_HELPER_UPUMP_MGR(upipe_bmd_sink_sub, upump_mgr);
UPIPE_HELPER_UPUMP(upipe_bmd_sink_sub, upump, upump_mgr);
UPIPE_HELPER_INPUT(upipe_bmd_sink_sub, urefs, nb_urefs, max_urefs, blockers, upipe_bmd_sink_sub_output);

UBASE_FROM_TO(upipe_bmd_sink, upipe_mgr, sub_mgr, sub_mgr)
UBASE_FROM_TO(upipe_bmd_sink, upipe_bmd_sink_sub, pic_subpipe, pic_subpipe)
UBASE_FROM_TO(upipe_bmd_sink, upipe_bmd_sink_sub, sound_subpipe, sound_subpipe)
UBASE_FROM_TO(upipe_bmd_sink, upipe_bmd_sink_sub, subpic_subpipe, subpic_subpipe)

UBASE_FROM_TO(upipe_bmd_sink, uclock_bmd_sink, uclock_bmd_sink, uclock)

static const bool parity_tab[256] =
{
#   define P2(n) n, n^1, n^1, n
#   define P4(n) P2(n), P2(n^1), P2(n^1), P2(n)
#   define P6(n) P4(n), P4(n^1), P4(n^1), P4(n)
    P6(0), P6(1), P6(1), P6(0)
};

#define ANC_START_LEN   6
#define CDP_HEADER_SIZE 7

static void upipe_bmd_sink_clear_vanc(uint16_t *dst, int w, int sd)
{
    int i;
    if (sd){
        // uyvy
        for (i = 0; i < (w/4); i++) {
            dst[0] = 0x200;
            dst[1] = 0x40;
            dst[2] = 0x200;
            dst[3] = 0x40;
            dst += 4;
        }
    }
    else {
        // nv20
        for (i = 0; i < (w/2); i++)
            dst[i] = 0x40;

        dst += (w/2);

        for (i = 0; i < (w/2); i++)
            dst[i] = 0x200;
    }
}

/* XXX: put this somewhere */
static void upipe_bmd_sink_start_anc(struct upipe *upipe, uint16_t *dst,
                                     uint16_t did, uint16_t sdid)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);

    /* reset dc */
    upipe_bmd_sink->dc = 0;

    /* ADF */
    dst[0] = 0x000;
    dst[1] = 0x3ff;
    dst[2] = 0x3ff;
    /* DID */
    dst[3] = did;
    /* SDID */
    dst[4] = sdid;
    /* DC */
    dst[5] = 0;
    upipe_bmd_sink->dc = &dst[5];
}

static void upipe_bmd_sink_write_cdp_header(struct upipe *upipe, uint16_t *dst)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);

    /** XXX: Support crazy 25fps captions? **/
    uint8_t fps = !strcmp(upipe_bmd_sink->mode, "ntsc") ||
                  !strcmp(upipe_bmd_sink->mode, "Hi59") ? 0x4 : 0x7;

    dst[0] = 0x96;
    dst[1] = 0x69;
    dst[2] = 0; // cdp_length
    dst[3] = (fps << 4) | 0xf; // cdp_frame_rate | Reserved
    dst[4] = (1 << 8) | (1 << 1) | 1; // ccdata_present | caption_service_active | Reserved
    dst[5] = upipe_bmd_sink->cdp_hdr_sequence_cntr >> 8;
    dst[6] = upipe_bmd_sink->cdp_hdr_sequence_cntr & 0xff;

    upipe_bmd_sink->cdp_hdr_sequence_cntr++;

    *upipe_bmd_sink->dc += CDP_HEADER_SIZE;
}

static void upipe_bmd_sink_write_ccdata_section(struct upipe *upipe, uint16_t *dst,
                                                const uint8_t *src, size_t src_size)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);
    size_t i;

    dst[0] = 0x72;
    dst[1] = (0x7 << 5) | (src_size / 3);
    dst += 2;

    for (i = 0; i < src_size; i++)
        dst[i] = src[i];

    *upipe_bmd_sink->dc += src_size+2;
}

static void upipe_bmd_sink_write_cdp_footer(struct upipe *upipe, uint16_t *dst)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);
    uint8_t checksum = 0, cnt = 0;
    int i;

    dst[0] = 0x74;
    dst[1] = upipe_bmd_sink->cdp_hdr_sequence_cntr >> 8;
    dst[2] = upipe_bmd_sink->cdp_hdr_sequence_cntr & 0xff;

    *upipe_bmd_sink->dc += 4;
    cnt = *upipe_bmd_sink->dc;
    upipe_bmd_sink->vanc_tmp[ANC_START_LEN+2] = cnt; // set cdp length

    for( i = 0; i < cnt-1; i++ ) // don't include checksum
        checksum += upipe_bmd_sink->vanc_tmp[ANC_START_LEN+i];

    dst[3] = checksum ? 256 - checksum : 0;
}

static void upipe_bmd_sink_write_cdp(struct upipe *upipe, const uint8_t *src,
                                     size_t src_size, uint16_t *dst)
{
    upipe_bmd_sink_write_cdp_header(upipe, dst);
    upipe_bmd_sink_write_ccdata_section(upipe, &dst[CDP_HEADER_SIZE], src, src_size);
    upipe_bmd_sink_write_cdp_footer(upipe, &dst[CDP_HEADER_SIZE+src_size+2]);
}

static void upipe_bmd_sink_calc_parity_checksum(struct upipe *upipe)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);
    uint16_t i, dc = *upipe_bmd_sink->dc;
    uint16_t checksum = 0;

    /* +3 = did + sdid + dc itself */
    for( i = 0; i < dc+3; i++ )
    {
        uint8_t parity = parity_tab[upipe_bmd_sink->vanc_tmp[3+i] & 0xff];
        upipe_bmd_sink->vanc_tmp[3+i] |= (!parity << 9) | (parity << 8);

        checksum += upipe_bmd_sink->vanc_tmp[3+i] & 0x1ff;
    }

    checksum &= 0x1ff;
    checksum |= (!(checksum >> 8)) << 9;

    upipe_bmd_sink->vanc_tmp[ANC_START_LEN+dc] = checksum;
}

static void upipe_bmd_sink_encode_v210(struct upipe *upipe, uint32_t *dst, int sd)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);
    int width = upipe_bmd_sink->displayMode->GetWidth();
    int w;
    uint32_t val = 0;

    /* FIXME: SIMD */
    if (sd) {
        uint16_t *u = &upipe_bmd_sink->vanc_tmp[0];
        uint16_t *y = &upipe_bmd_sink->vanc_tmp[1];

        /* Guaranteed mod-6 width */
        for( w = 0; w < width; w += 6 ){
            WRITE_PIXELS(u, y, (u+2));
            u += 4;
            y += 2;
            WRITE_PIXELS(y, u, (y+2));
            u += 2;
            y += 4;
            WRITE_PIXELS(u, y, (u+2));
            u += 4;
            y += 2;
            WRITE_PIXELS(y, u, (y+2));
            u += 2;
            y += 4;
        }
    }
    else {
        /* 1280 isn't mod-6 so long vanc packets will be truncated */
        uint16_t *y = &upipe_bmd_sink->vanc_tmp[0];
        uint16_t *u = &upipe_bmd_sink->vanc_tmp[width];

        for( w = 0; w < width; w += 6 ){
            WRITE_PIXELS(u, y, (u+1));
            y += 1;
            u += 2;
            WRITE_PIXELS(y, u, (y+1));
            y += 2;
            u += 1;
            WRITE_PIXELS(u, y, (u+1));
            y += 1;
            u += 2;
            WRITE_PIXELS(y, u, (y+1));
            y += 2;
            u += 1;
        }
    }
}

/** @internal @This initializes an subpipe of a bmd sink pipe.
 *
 * @param upipe pointer to subpipe
 * @param sub_mgr manager of the subpipe
 * @param uprobe structure used to raise events by the subpipe
 */
static void upipe_bmd_sink_sub_init(struct upipe *upipe,
        struct upipe_mgr *sub_mgr, struct uprobe *uprobe)
{
    upipe_init(upipe, sub_mgr, uprobe);

    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_sub_mgr(sub_mgr);
    struct upipe_bmd_sink_sub *upipe_bmd_sink_sub = upipe_bmd_sink_sub_from_upipe(upipe);
    upipe_bmd_sink_sub->upipe_bmd_sink = upipe_bmd_sink_to_upipe(upipe_bmd_sink);

    upipe_bmd_sink_sub_init_urefcount(upipe);
    upipe_bmd_sink_sub_init_input(upipe);
    upipe_bmd_sink_sub_init_upump_mgr(upipe);
    upipe_bmd_sink_sub_init_upump(upipe);

    upipe_throw_ready(upipe);
}

static void upipe_bmd_sink_sub_free(struct upipe *upipe)
{
    struct upipe_bmd_sink_sub *upipe_bmd_sink_sub = upipe_bmd_sink_sub_from_upipe(upipe);
    upipe_throw_dead(upipe);
    upipe_bmd_sink_sub_clean_upump(upipe);
    upipe_bmd_sink_sub_clean_upump_mgr(upipe);
    upipe_bmd_sink_sub_clean_input(upipe);
    upipe_bmd_sink_sub_clean_urefcount(upipe);
    upipe_release(upipe_bmd_sink_sub->upipe_bmd_sink);
}

/** @internal @This is called when the data should be displayed.
 *
 * @param upump description structure of the watcher
 */
static void upipe_bmd_sink_sub_write_watcher(struct upump *upump)
{
    struct upipe *upipe = upump_get_opaque(upump, struct upipe *);
    upipe_bmd_sink_sub_set_upump(upipe, NULL);
    upipe_bmd_sink_sub_output_input(upipe);
    upipe_bmd_sink_sub_unblock_input(upipe);
}

/** @internal @This handles input uref.
 *
 * @param upipe description structure of the pipe
 * @param uref uref structure
 * @param upump_p reference to upump structure
 */
static bool upipe_bmd_sink_sub_output(struct upipe *upipe, struct uref *uref,
                                      struct upump **upump_p)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);
    struct upipe_bmd_sink_sub *upipe_bmd_sink_sub =
        upipe_bmd_sink_sub_from_upipe(upipe);

    HRESULT result;
    IDeckLinkVideoFrameAncillary *ancillary = NULL;

    const char *def;
    if (unlikely(ubase_check(uref_flow_get_def(uref, &def)))) {
        upipe_bmd_sink_sub->latency = 0;
        uref_clock_get_latency(uref, &upipe_bmd_sink_sub->latency);

        upipe_bmd_sink_sub_check_upump_mgr(upipe);

        uref_free(uref);
        return true;
    }

    uint64_t pts = 0;
    if (likely(ubase_check(uref_clock_get_pts_sys(uref, &pts)))) {
        uint64_t now = uclock_now(&upipe_bmd_sink->uclock.uclock);
        pts += upipe_bmd_sink_sub->latency;

        if (now < pts) {
            upipe_verbose_va(upipe, "sleeping %"PRIu64" (%"PRIu64")",
                             pts - now, pts);
            upipe_bmd_sink_sub_wait_upump(upipe, pts - now,
                                          upipe_bmd_sink_sub_write_watcher);
            return false;
        } else if (now > pts + UCLOCK_FREQ / 10) {
            upipe_warn_va(upipe, "late uref dropped (%"PRId64")",
                          (now - pts) * 1000 / UCLOCK_FREQ);
            uref_free(uref);
            return true;
        }
    }

    if (upipe_bmd_sink_sub == &upipe_bmd_sink->pic_subpipe) {
        int w = upipe_bmd_sink->displayMode->GetWidth();
        int h = upipe_bmd_sink->displayMode->GetHeight();
        int sd = !strcmp(upipe_bmd_sink->mode, "pal ") || !strcmp(upipe_bmd_sink->mode, "ntsc");
        const uint8_t *pic_data = NULL;
        size_t pic_data_size = 0;

        if(!upipe_bmd_sink->started && pts > 0) {
            upipe_bmd_sink->deckLinkOutput->StartScheduledPlayback(pts, UCLOCK_FREQ, 1.0);
            upipe_bmd_sink->started = 1;
        }

        const char *v210 = "u10y10v10y10u10y10v10y10u10y10v10y10";
        size_t stride;
        const uint8_t *plane;
        if (unlikely(!ubase_check(uref_pic_plane_size(uref, v210, &stride,
                                          NULL, NULL, NULL)) ||
                     !ubase_check(uref_pic_plane_read(uref, v210, 0, 0, -1, -1,
                                          &plane)))) {
            upipe_err_va(upipe, "Could not read v210 plane");
            return NULL;
        }

        upipe_bmd_sink_frame *video_frame = new upipe_bmd_sink_frame(uref, (void*)plane,
                                                                     w, h);
        if (!video_frame) {
            uref_free(uref);
            return true;
        }

        BMDTimeValue timeValue;
        BMDTimeScale timeScale;
        upipe_bmd_sink->displayMode->GetFrameRate(&timeValue, &timeScale);

        upipe_bmd_sink->deckLinkOutput->CreateAncillaryData(video_frame->GetPixelFormat(), &ancillary);

        uref_pic_get_cea_708(uref, &pic_data, &pic_data_size);
        if( pic_data_size > 0 )
        {
            void *vanc;
            ancillary->GetBufferForVerticalBlankingLine(CC_LINE, &vanc);
            upipe_bmd_sink_clear_vanc(upipe_bmd_sink->vanc_tmp, w, sd);
            upipe_bmd_sink_start_anc(upipe, upipe_bmd_sink->vanc_tmp, 0x61, 0x1);
            upipe_bmd_sink_write_cdp(upipe, pic_data, pic_data_size, &upipe_bmd_sink->vanc_tmp[ANC_START_LEN]);
            upipe_bmd_sink_calc_parity_checksum(upipe);

            upipe_bmd_sink_encode_v210(upipe, (uint32_t*)vanc, sd);
        }

        video_frame->SetAncillaryData(ancillary);

        if( pts > 0 )
        {
            result = upipe_bmd_sink->deckLinkOutput->ScheduleVideoFrame(video_frame, pts, UCLOCK_FREQ * timeValue / timeScale, UCLOCK_FREQ);
            if( result != S_OK )
                upipe_err_va(upipe, "DROPPED FRAME %x", result);
        }

        video_frame->Release();
    }
    else if (upipe_bmd_sink_sub == &upipe_bmd_sink->sound_subpipe && upipe_bmd_sink->started) {
        size_t size = 0;
        uref_sound_size(uref, &size, NULL);
        const int32_t *buffers[1];
        uref_sound_read_int32_t(uref, 0, -1, buffers, 1);

        uint32_t written;
        result = upipe_bmd_sink->deckLinkOutput->ScheduleAudioSamples((void*)buffers[0], size, pts, UCLOCK_FREQ, &written);
        uref_sound_unmap(uref, 0, -1, 1);

        if( result != S_OK )
            upipe_err_va(upipe, "DROPPED AUDIO");

        uint32_t buffered;
        upipe_bmd_sink->deckLinkOutput->GetBufferedAudioSampleFrameCount(&buffered);

        if (buffered == 0) {
            /* TODO: get notified as soon as audio buffers empty */
            upipe_bmd_sink->deckLinkOutput->StopScheduledPlayback(0, NULL, 0);
            upipe_bmd_sink->started = 0;
        }
        if (written != size)
            upipe_dbg_va(upipe, "written %u/%u", written, size);
        upipe_dbg_va(upipe, "buffered samples: %u", buffered );
        uref_free(uref);
    }

    return true;
}

/** @internal @This handles output data.
 *
 * @param upipe description structure of the pipe
 * @param uref uref structure
 * @param upump_p reference to upump structure
 */
static void upipe_bmd_sink_sub_input(struct upipe *upipe, struct uref *uref,
                                      struct upump **upump_p)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);

    if (!upipe_bmd_sink->deckLink) {
        upipe_err_va(upipe, "DeckLink card not ready");
        return;
    }

    if (!upipe_bmd_sink_sub_check_input(upipe) ||
        !upipe_bmd_sink_sub_output(upipe, uref, upump_p)) {
        upipe_bmd_sink_sub_hold_input(upipe, uref);
        upipe_bmd_sink_sub_block_input(upipe, upump_p);
    }
}

/** @internal @This sets the input flow definition.
 *
 * @param upipe description structure of the pipe
 * @param flow_def flow definition packet
 * @return an error code
 */
static int upipe_bmd_sink_sub_set_flow_def(struct upipe *upipe,
                                          struct uref *flow_def)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);
    struct upipe_bmd_sink_sub *upipe_bmd_sink_sub =
        upipe_bmd_sink_sub_from_upipe(upipe);

    if (flow_def == NULL)
        return UBASE_ERR_INVALID;

    if (upipe_bmd_sink_sub == &upipe_bmd_sink->pic_subpipe) {
        uint8_t macropixel;
        if (!ubase_check(uref_pic_flow_get_macropixel(flow_def, &macropixel))) {
            upipe_err(upipe, "macropixel size not set");
            uref_dump(flow_def, upipe->uprobe);
            return UBASE_ERR_EXTERNAL;
        }

        if (macropixel != 48 || ubase_check(
                             uref_pic_flow_check_chroma(flow_def, 1, 1, 1,
                                                        "u10y10v10y10u10y10v10y10u10y10v10y10"))) {
            upipe_err(upipe, "incompatible input flow def");
            uref_dump(flow_def, upipe->uprobe);
            return UBASE_ERR_EXTERNAL;
        }
    }

    flow_def = uref_dup(flow_def);
    UBASE_ALLOC_RETURN(flow_def)
    upipe_input(upipe, flow_def, NULL);
    return UBASE_ERR_NONE;
}

/** @internal @This processes control commands on an output subpipe of an
 * bmd_sink pipe.
 *
 * @param upipe description structure of the pipe
 * @param command type of command to process
 * @param args arguments of the command
 * @return an error code
 */
static int upipe_bmd_sink_sub_control(struct upipe *upipe,
                                     int command, va_list args)
{
    switch (command) {
        case UPIPE_ATTACH_UPUMP_MGR: {
            upipe_bmd_sink_sub_set_upump(upipe, NULL);
            UBASE_RETURN(upipe_bmd_sink_sub_attach_upump_mgr(upipe))
            return UBASE_ERR_NONE;
        }
        case UPIPE_REGISTER_REQUEST: {
            struct urequest *request = va_arg(args, struct urequest *);
            return upipe_throw_provide_request(upipe, request);
        }
        case UPIPE_UNREGISTER_REQUEST:
            return UBASE_ERR_NONE;
        case UPIPE_SET_FLOW_DEF: {
            struct uref *flow_def = va_arg(args, struct uref *);
            return upipe_bmd_sink_sub_set_flow_def(upipe, flow_def);
        }
        case UPIPE_SUB_GET_SUPER: {
            struct upipe **p = va_arg(args, struct upipe **);
            *p = upipe_bmd_sink_to_upipe(upipe_bmd_sink_from_sub_mgr(upipe->mgr));
        }

        default:
            return UBASE_ERR_UNHANDLED;
    }
}

/** @internal @This initializes the output manager for an bmd_sink pipe.
 *
 * @param upipe description structure of the pipe
 */
static void upipe_bmd_sink_init_sub_mgr(struct upipe *upipe)
{
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_upipe(upipe);
    struct upipe_mgr *sub_mgr = &upipe_bmd_sink->sub_mgr;
    sub_mgr->refcount = upipe_bmd_sink_to_urefcount(upipe_bmd_sink);
    sub_mgr->signature = UPIPE_BMD_SINK_INPUT_SIGNATURE;
    sub_mgr->upipe_alloc = NULL;
    sub_mgr->upipe_input = upipe_bmd_sink_sub_input;
    sub_mgr->upipe_control = upipe_bmd_sink_sub_control;
    sub_mgr->upipe_mgr_control = NULL;
}

/** @internal @This allocates a bmd_sink pipe.
 *
 * @param mgr common management structure
 * @param uprobe structure used to raise events
 * @param signature signature of the pipe allocator
 * @param args optional arguments
 * @return pointer to upipe or NULL in case of allocation error
 */
static struct upipe *upipe_bmd_sink_alloc(struct upipe_mgr *mgr,
                                      struct uprobe *uprobe,
                                      uint32_t signature, va_list args)
{
    if (signature != UPIPE_BMD_SINK_SIGNATURE)
        return NULL;
    struct uprobe *uprobe_pic = va_arg(args, struct uprobe *);
    struct uprobe *uprobe_sound = va_arg(args, struct uprobe *);
    struct uprobe *uprobe_subpic = va_arg(args, struct uprobe *);

    struct upipe_bmd_sink *upipe_bmd_sink =
        (struct upipe_bmd_sink *)calloc(1, sizeof(struct upipe_bmd_sink));
    if (unlikely(upipe_bmd_sink == NULL)) {
        uprobe_release(uprobe_pic);
        uprobe_release(uprobe_sound);
        uprobe_release(uprobe_subpic);
        return NULL;
    }

    struct upipe *upipe = upipe_bmd_sink_to_upipe(upipe_bmd_sink);
    upipe_init(upipe, mgr, uprobe);

    upipe_bmd_sink_init_sub_mgr(upipe);
    upipe_bmd_sink_init_urefcount(upipe);

    /* Initalise subpipes */
    upipe_bmd_sink_sub_init(upipe_bmd_sink_sub_to_upipe(upipe_bmd_sink_to_pic_subpipe(upipe_bmd_sink)),
                            &upipe_bmd_sink->sub_mgr, uprobe_pic);
    upipe_bmd_sink_sub_init(upipe_bmd_sink_sub_to_upipe(upipe_bmd_sink_to_sound_subpipe(upipe_bmd_sink)),
                            &upipe_bmd_sink->sub_mgr, uprobe_sound);
    upipe_bmd_sink_sub_init(upipe_bmd_sink_sub_to_upipe(upipe_bmd_sink_to_subpic_subpipe(upipe_bmd_sink)),
                            &upipe_bmd_sink->sub_mgr, uprobe_subpic);

    upipe_throw_ready(upipe);
    return upipe;
}

/** @This returns the Blackmagic hardware output time.
 *
 * @param uclock utility structure passed to the module
 * @return current hardware output time in 27 MHz ticks
 */
static uint64_t uclock_bmd_sink_now(struct uclock *uclock)
{
    struct uclock_bmd_sink *uclock_bmd_sink = uclock_bmd_sink_from_uclock(uclock);
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_uclock_bmd_sink(uclock_bmd_sink);

    BMDTimeValue hardware_time, time_in_frame, ticks_per_frame;

    if (upipe_bmd_sink->deckLinkOutput) {
        upipe_bmd_sink->deckLinkOutput->GetHardwareReferenceClock(UCLOCK_FREQ, &hardware_time,
                                                                  &time_in_frame, &ticks_per_frame);
    }

    return (uint64_t)hardware_time + uclock_bmd_sink->offset;
}

static void uclock_set_offset(struct uclock *uclock)
{
    struct uclock_bmd_sink *uclock_bmd_sink = uclock_bmd_sink_from_uclock(uclock);

    uclock_bmd_sink->offset += uclock_bmd_sink_now(uclock);
}

/** @This frees a uclock.
 *
 * @param urefcount pointer to urefcount
 */
static void uclock_bmd_sink_free(struct urefcount *urefcount)
{
    struct uclock_bmd_sink *uclock_bmd_sink = uclock_bmd_sink_from_urefcount(urefcount);

    urefcount_clean(urefcount);
    uclock_bmd_sink->uclock.uclock_now = NULL;
}

static void uclock_bmd_sink_init(uclock_bmd_sink *uclock_bmd_sink)
{
    urefcount_init(uclock_bmd_sink_to_urefcount(uclock_bmd_sink), uclock_bmd_sink_free);
    uclock_bmd_sink->uclock.refcount = uclock_bmd_sink_to_urefcount(uclock_bmd_sink);
    uclock_bmd_sink->uclock.uclock_now = uclock_bmd_sink_now;
}

/** @internal @This asks to open the given device.
 *
 * @param upipe description structure of the pipe
 * @param uri URI
 * @return an error code
 */
static int upipe_bmd_sink_set_uri(struct upipe *upipe, const char *uri)
{
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_upipe(upipe);

    IDeckLinkIterator *deckLinkIterator = NULL;
    IDeckLink *deckLink = NULL;
    IDeckLinkOutput *deckLinkOutput;
    IDeckLinkDisplayModeIterator *displayModeIterator = NULL;
    char* displayModeName = NULL;
    IDeckLinkDisplayMode* displayMode = NULL;
    int err = UBASE_ERR_NONE;
    HRESULT result = E_NOINTERFACE;

    if (upipe_bmd_sink->deckLink){
        uclock_set_offset(&upipe_bmd_sink->uclock.uclock);
        upipe_bmd_sink_sub_flush_input(&upipe_bmd_sink->pic_subpipe.upipe);
        upipe_bmd_sink_sub_flush_input(&upipe_bmd_sink->sound_subpipe.upipe);
        upipe_bmd_sink_sub_flush_input(&upipe_bmd_sink->subpic_subpipe.upipe);
        upipe_bmd_sink->deckLinkOutput->StopScheduledPlayback(0, NULL, 0);
        upipe_bmd_sink->deckLinkOutput->DisableVideoOutput();
        upipe_bmd_sink->deckLinkOutput->DisableAudioOutput();
        upipe_bmd_sink->displayMode->Release();
        upipe_bmd_sink->deckLink->Release();
        upipe_bmd_sink->started = 0;
    }

    /* decklink interface interator */
    deckLinkIterator = CreateDeckLinkIteratorInstance();
    if (!deckLinkIterator) {
        upipe_err_va(upipe, "decklink drivers not found");
        err = UBASE_ERR_EXTERNAL;
        goto end;
    }

    /* get decklink interface handler */
    for (int i = 0; i <= upipe_bmd_sink->card_idx; i++) {
        if (deckLink)
            deckLink->Release();
        result = deckLinkIterator->Next(&deckLink);
        if (result != S_OK)
            break;
    }

    if (result != S_OK) {
        upipe_err_va(upipe, "decklink card %d not found", upipe_bmd_sink->card_idx);
        err = UBASE_ERR_EXTERNAL;
        goto end;
    }

    if (deckLink->QueryInterface(IID_IDeckLinkOutput,
                                 (void**)&deckLinkOutput) != S_OK) {
        upipe_err_va(upipe, "decklink card has no output");
        err = UBASE_ERR_EXTERNAL;
        goto end;
    }

    result = deckLinkOutput->GetDisplayModeIterator(&displayModeIterator);
    if (result != S_OK){
        upipe_err_va(upipe, "decklink card has no display modes");
        err = UBASE_ERR_EXTERNAL;
        goto end;
    }

    while ((result = displayModeIterator->Next(&displayMode)) == S_OK)
    {
        union {
            BMDDisplayMode mode_id;
            char mode_s[4];
        } u;
        u.mode_id = ntohl(displayMode->GetDisplayMode());

        if (!strncmp(u.mode_s, upipe_bmd_sink->mode, 4))
            break;

        displayMode->Release();
    }

    if (result != S_OK || displayMode == NULL)
    {
        fprintf(stderr, "Unable to get display mode %s\n", upipe_bmd_sink->mode);
        err = UBASE_ERR_EXTERNAL;
        goto end;
    }

    result = displayMode->GetName((const char**)&displayModeName);
    if (result == S_OK)
    {
        upipe_dbg_va(upipe, "Using mode %s", displayModeName);
        free(displayModeName);
    }

    upipe_bmd_sink->displayMode = displayMode;

    result = deckLinkOutput->EnableVideoOutput(displayMode->GetDisplayMode(),
                                               bmdVideoOutputVANC);
    if (result != S_OK)
    {
        fprintf(stderr, "Failed to enable video output. Is another application using the card?\n");
        err = UBASE_ERR_EXTERNAL;
        goto end;
    }

    result = deckLinkOutput->EnableAudioOutput(48000, bmdAudioSampleType32bitInteger, 16, bmdAudioOutputStreamTimestamped);
    if (result != S_OK)
    {
        fprintf(stderr, "Failed to enable audio output. Is another application using the card?\n");
        err = UBASE_ERR_EXTERNAL;
        goto end;
    }

    if (!upipe_bmd_sink->deckLink)
    {
        uclock_bmd_sink_init(&upipe_bmd_sink->uclock);
        urefcount_use(uclock_bmd_sink_to_urefcount(&upipe_bmd_sink->uclock));
    }

    upipe_bmd_sink->deckLinkOutput = deckLinkOutput;
    upipe_bmd_sink->deckLink = deckLink;

end:

    if (displayModeIterator != NULL)
        displayModeIterator->Release();

    if (deckLinkIterator)
        deckLinkIterator->Release();

    return err;
}

/** @internal @This sets the content of a bmd_sink option.
 *
 * @param upipe description structure of the pipe
 * @param option name of the option
 * @param content content of the option, or NULL to delete it
 * @return an error code
 */
static int upipe_bmd_sink_set_option(struct upipe *upipe,
                                   const char *k, const char *v)
{
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_upipe(upipe);
    assert(k != NULL);

    if (!strcmp(k, "card-idx"))
        upipe_bmd_sink->card_idx = atoi(v);

    if (!strcmp(k, "mode")) {
        strncpy(upipe_bmd_sink->mode, v, sizeof(upipe_bmd_sink->mode));
        upipe_bmd_sink->mode[4] = '\0';
    }

    return UBASE_ERR_NONE;
}

/** @internal @This returns the bmd_sink genlock status. 
 *
 * @param upipe description structure of the pipe
 * @param pointer to integer for genlock status
 * @return an error code
 */
static int _upipe_bmd_sink_get_genlock_status(struct upipe *upipe, int *status)
{
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_upipe(upipe);
    BMDReferenceStatus reference_status;

    HRESULT result = upipe_bmd_sink->deckLinkOutput->GetReferenceStatus(&reference_status);
    if (reference_status & bmdReferenceNotSupportedByHardware) {
        *status = UPIPE_BMD_SINK_GENLOCK_UNSUPPORTED;
        return UBASE_ERR_NONE;
    }

    if (reference_status & bmdReferenceLocked) {
        *status = UPIPE_BMD_SINK_GENLOCK_LOCKED;
        return UBASE_ERR_NONE;
    }

    *status = UPIPE_BMD_SINK_GENLOCK_UNLOCKED;
    return UBASE_ERR_NONE;
}

/** @internal @This returns the bmd_sink genlock offset. 
 *
 * @param upipe description structure of the pipe
 * @param pointer to int64_t for genlock offset
 * @return an error code
 */
static int _upipe_bmd_sink_get_genlock_offset(struct upipe *upipe, int64_t *offset)
{
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_upipe(upipe);
    BMDReferenceStatus reference_status;
    IDeckLinkConfiguration *decklink_configuration;
    HRESULT result;

    result = upipe_bmd_sink->deckLinkOutput->GetReferenceStatus(&reference_status);
    if ((reference_status & bmdReferenceNotSupportedByHardware) ||
        !(reference_status & bmdReferenceLocked)) {
        *offset = 0;
        return UBASE_ERR_EXTERNAL;
    }

    result = upipe_bmd_sink->deckLink->QueryInterface(IID_IDeckLinkConfiguration, (void**)&decklink_configuration);
    if (result != S_OK) {
        *offset = 0;
        return UBASE_ERR_EXTERNAL;
    }

    result = decklink_configuration->GetInt(bmdDeckLinkConfigReferenceInputTimingOffset, offset);
    if (result != S_OK) {
        *offset = 0;
        decklink_configuration->Release();
        return UBASE_ERR_EXTERNAL;
    }
    decklink_configuration->Release();

    return UBASE_ERR_NONE;
}

/** @internal @This sets the bmd_sink genlock offset. 
 *
 * @param upipe description structure of the pipe
 * @param int64_t requested genlock offset
 * @return an error code
 */
static int _upipe_bmd_sink_set_genlock_offset(struct upipe *upipe, int64_t offset)
{
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_upipe(upipe);
    BMDReferenceStatus reference_status;
    IDeckLinkConfiguration *decklink_configuration;
    HRESULT result;

    result = upipe_bmd_sink->deckLinkOutput->GetReferenceStatus(&reference_status);
    if ((reference_status & bmdReferenceNotSupportedByHardware)) {
        return UBASE_ERR_EXTERNAL;
    }

    result = upipe_bmd_sink->deckLink->QueryInterface(IID_IDeckLinkConfiguration, (void**)&decklink_configuration);
    if (result != S_OK) {
        return UBASE_ERR_EXTERNAL;
    }

    result = decklink_configuration->SetInt(bmdDeckLinkConfigReferenceInputTimingOffset, offset);
    if (result != S_OK) {
        decklink_configuration->Release();
        return UBASE_ERR_EXTERNAL;
    }

    decklink_configuration->Release();

    return UBASE_ERR_NONE;
}


/** @internal @This processes control commands on an bmd_sink source pipe.
 *
 * @param upipe description structure of the pipe
 * @param command type of command to process
 * @param args arguments of the command
 * @return an error code
 */
static int upipe_bmd_sink_control(struct upipe *upipe, int command, va_list args)
{
    switch (command) {
        case UPIPE_SET_URI: {
            const char *uri = va_arg(args, const char *);
            return upipe_bmd_sink_set_uri(upipe, uri);
        }
        case UPIPE_BMD_SINK_GET_PIC_SUB: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_BMD_SINK_SIGNATURE)
            struct upipe **upipe_p = va_arg(args, struct upipe **);
            *upipe_p =  upipe_bmd_sink_sub_to_upipe(
                            upipe_bmd_sink_to_pic_subpipe(
                                upipe_bmd_sink_from_upipe(upipe)));
            return UBASE_ERR_NONE;
        }
        case UPIPE_BMD_SINK_GET_SOUND_SUB: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_BMD_SINK_SIGNATURE)
            struct upipe **upipe_p = va_arg(args, struct upipe **);
            *upipe_p =  upipe_bmd_sink_sub_to_upipe(
                            upipe_bmd_sink_to_sound_subpipe(
                                upipe_bmd_sink_from_upipe(upipe)));
            return UBASE_ERR_NONE;
        }
        case UPIPE_BMD_SINK_GET_SUBPIC_SUB: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_BMD_SINK_SIGNATURE)
            struct upipe **upipe_p = va_arg(args, struct upipe **);
            *upipe_p =  upipe_bmd_sink_sub_to_upipe(
                            upipe_bmd_sink_to_subpic_subpipe(
                                upipe_bmd_sink_from_upipe(upipe)));
            return UBASE_ERR_NONE;
        }
        case UPIPE_BMD_SINK_GET_UCLOCK: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_BMD_SINK_SIGNATURE)
            struct uclock **pp_uclock = va_arg(args, struct uclock **);
            struct upipe_bmd_sink *bmd_sink = upipe_bmd_sink_from_upipe(upipe);
            struct uclock_bmd_sink *uclock_bmd_sink = upipe_bmd_sink_to_uclock_bmd_sink(bmd_sink);
            struct uclock *uclock = uclock_bmd_sink_to_uclock(uclock_bmd_sink);
            *pp_uclock = uclock;
            return UBASE_ERR_NONE;
        }
        case UPIPE_BMD_SINK_GET_GENLOCK_STATUS: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_BMD_SINK_SIGNATURE)
            int *status = va_arg(args, int *);
            return _upipe_bmd_sink_get_genlock_status(upipe, status);
        }
        case UPIPE_BMD_SINK_GET_GENLOCK_OFFSET: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_BMD_SINK_SIGNATURE)
            int64_t *offset = va_arg(args, int64_t *);
            return _upipe_bmd_sink_get_genlock_offset(upipe, offset);
        }
        case UPIPE_BMD_SINK_SET_GENLOCK_OFFSET: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_BMD_SINK_SIGNATURE)
            int64_t offset = va_arg(args, int64_t);
            return _upipe_bmd_sink_set_genlock_offset(upipe, offset);
        }
        case UPIPE_SET_OPTION: {
            const char *k = va_arg(args, const char *);
            const char *v = va_arg(args, const char *);
            return upipe_bmd_sink_set_option(upipe, k, v);
        }
        default:
            return UBASE_ERR_UNHANDLED;
    }
}

/** @internal @This frees all resources allocated.
 *
 * @param upipe description structure of the pipe
 */
static void upipe_bmd_sink_free(struct upipe *upipe)
{
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_upipe(upipe);
    struct uclock_bmd_sink *uclock_bmd_sink = &upipe_bmd_sink->uclock;

    upipe_release(upipe_bmd_sink_sub_to_upipe(&upipe_bmd_sink->pic_subpipe));
    upipe_release(upipe_bmd_sink_sub_to_upipe(&upipe_bmd_sink->sound_subpipe));
    upipe_release(upipe_bmd_sink_sub_to_upipe(&upipe_bmd_sink->subpic_subpipe));
    upipe_throw_dead(upipe);

    urefcount_release(uclock_bmd_sink_to_urefcount(uclock_bmd_sink));

    if (upipe_bmd_sink->deckLink) {
        upipe_bmd_sink->deckLinkOutput->StopScheduledPlayback(0, NULL, 0);
        upipe_bmd_sink->deckLinkOutput->DisableVideoOutput();
        upipe_bmd_sink->deckLinkOutput->DisableAudioOutput();
        upipe_bmd_sink->deckLinkOutput->Release();
        upipe_bmd_sink->displayMode->Release();
        upipe_bmd_sink->deckLink->Release();
    }

    upipe_dbg_va(upipe, "releasing blackmagic sink pipe %p", upipe);

    upipe_bmd_sink_clean_urefcount(upipe);
    upipe_bmd_sink_free_void(upipe);
}

/** upipe_bmd_sink (/dev/bmd_sink) */
static struct upipe_mgr upipe_bmd_sink_mgr = {
    /* .refcount = */ NULL,
    /* .signature = */ UPIPE_BMD_SINK_SIGNATURE,

    /* .upipe_err_str = */ NULL,
    /* .upipe_command_str = */ NULL,
    /* .upipe_event_str = */ NULL,

    /* .upipe_alloc = */ upipe_bmd_sink_alloc,
    /* .upipe_input = */ NULL,
    /* .upipe_control = */ upipe_bmd_sink_control,

    /* .upipe_mgr_control = */ NULL
};

/** @This returns the management structure for bmd_sink pipes
 *
 * @return pointer to manager
 */
struct upipe_mgr *upipe_bmd_sink_mgr_alloc(void)
{
    return &upipe_bmd_sink_mgr;
}
