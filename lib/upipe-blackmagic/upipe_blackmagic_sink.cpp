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
#include <upipe/uref_block.h>
#include <upipe/uref_block_flow.h>
#include <upipe/uref_pic.h>
#include <upipe/uref_pic_flow.h>
#include <upipe/uref_sound.h>
#include <upipe/uref_sound_flow.h>
#include <upipe/uref_clock.h>
#include <upipe/uref_dump.h>
#include <upipe/upipe.h>
#include <upipe/upipe_helper_input.h>
#include <upipe/upipe_helper_flow.h>
#include <upipe/upipe_helper_upipe.h>
#include <upipe/upipe_helper_subpipe.h>
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
#include <libzvbi.h>

#include "include/DeckLinkAPI.h"

#define PREROLL_FRAMES 3

#define DECKLINK_CHANNELS 16

#define CC_LINE 9
#define AFD_LINE 11
#define OP47_LINE1 12
#define OP47_LINE2 (OP47_LINE1+563)

#define PAL_FIELD_OFFSET 313

#define ANC_START_LEN   6
#define CDP_HEADER_SIZE 7
#define OP47_INITIAL_WORDS 4
#define OP47_STRUCT_A_LEN 5
#define OP47_STRUCT_B_OFFSET (ANC_START_LEN+OP47_INITIAL_WORDS+OP47_STRUCT_A_LEN)

#define VANC_WIDTH 1920

/* don't clip the v210 anc data */
#define WRITE_PIXELS(a, b, c)           \
    do {                                \
        val =  (*a);                    \
        val |= (*b << 10)  |            \
               (*c << 20);              \
        AV_WL32(dst, val);              \
        dst++;                          \
    } while (0)

#define WRITE_PIXELS8(a, b, c)           \
    do {                                \
        val =  (*a << 2);               \
        val |= (*b << 12)  |            \
               (*c << 22);              \
        AV_WL32(dst, val);              \
        dst++;                          \
    } while (0)

const static uint8_t reverse_tab[256] = {
    0x00,0x80,0x40,0xC0,0x20,0xA0,0x60,0xE0,0x10,0x90,0x50,0xD0,0x30,0xB0,0x70,0xF0,
    0x08,0x88,0x48,0xC8,0x28,0xA8,0x68,0xE8,0x18,0x98,0x58,0xD8,0x38,0xB8,0x78,0xF8,
    0x04,0x84,0x44,0xC4,0x24,0xA4,0x64,0xE4,0x14,0x94,0x54,0xD4,0x34,0xB4,0x74,0xF4,
    0x0C,0x8C,0x4C,0xCC,0x2C,0xAC,0x6C,0xEC,0x1C,0x9C,0x5C,0xDC,0x3C,0xBC,0x7C,0xFC,
    0x02,0x82,0x42,0xC2,0x22,0xA2,0x62,0xE2,0x12,0x92,0x52,0xD2,0x32,0xB2,0x72,0xF2,
    0x0A,0x8A,0x4A,0xCA,0x2A,0xAA,0x6A,0xEA,0x1A,0x9A,0x5A,0xDA,0x3A,0xBA,0x7A,0xFA,
    0x06,0x86,0x46,0xC6,0x26,0xA6,0x66,0xE6,0x16,0x96,0x56,0xD6,0x36,0xB6,0x76,0xF6,
    0x0E,0x8E,0x4E,0xCE,0x2E,0xAE,0x6E,0xEE,0x1E,0x9E,0x5E,0xDE,0x3E,0xBE,0x7E,0xFE,
    0x01,0x81,0x41,0xC1,0x21,0xA1,0x61,0xE1,0x11,0x91,0x51,0xD1,0x31,0xB1,0x71,0xF1,
    0x09,0x89,0x49,0xC9,0x29,0xA9,0x69,0xE9,0x19,0x99,0x59,0xD9,0x39,0xB9,0x79,0xF9,
    0x05,0x85,0x45,0xC5,0x25,0xA5,0x65,0xE5,0x15,0x95,0x55,0xD5,0x35,0xB5,0x75,0xF5,
    0x0D,0x8D,0x4D,0xCD,0x2D,0xAD,0x6D,0xED,0x1D,0x9D,0x5D,0xDD,0x3D,0xBD,0x7D,0xFD,
    0x03,0x83,0x43,0xC3,0x23,0xA3,0x63,0xE3,0x13,0x93,0x53,0xD3,0x33,0xB3,0x73,0xF3,
    0x0B,0x8B,0x4B,0xCB,0x2B,0xAB,0x6B,0xEB,0x1B,0x9B,0x5B,0xDB,0x3B,0xBB,0x7B,0xFB,
    0x07,0x87,0x47,0xC7,0x27,0xA7,0x67,0xE7,0x17,0x97,0x57,0xD7,0x37,0xB7,0x77,0xF7,
    0x0F,0x8F,0x4F,0xCF,0x2F,0xAF,0x6F,0xEF,0x1F,0x9F,0x5F,0xDF,0x3F,0xBF,0x7F,0xFF,
};

#define REVERSE(x) reverse_tab[(x)]

class upipe_bmd_sink_frame : public IDeckLinkVideoFrame
{
public:
    upipe_bmd_sink_frame(struct uref *_uref, void *_buffer, long _width, long _height, uint64_t _pts) :
                         uref(_uref), data(_buffer), width(_width), height(_height), pts(_pts) {
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

public:
    uint64_t pts;

private:
    struct uref *uref;
    void *data;
    long width;
    long height;

    uatomic_uint32_t refcount;
    IDeckLinkVideoFrameAncillary *frame_anc;
};

static float dur_to_time(uint64_t dur)
{
    return (float)dur / UCLOCK_FREQ;
}

static float pts_to_time(uint64_t pts)
{
    static uint64_t first = 0;
    if (!first)
        first = pts;

    return dur_to_time(pts - first);
}

class callback : public IDeckLinkVideoOutputCallback
{
public:
    virtual HRESULT ScheduledFrameCompleted (/* in */ IDeckLinkVideoFrame *completedFrame, /* in */ BMDOutputFrameCompletionResult result) {
        static const char *res[] = {
            "completed",
            "late",
            "dropped",
            "flushed",
        };

        if (result >= 4)
            abort();

        BMDTimeValue val;
        if (output->GetFrameCompletionReferenceTimestamp(completedFrame, UCLOCK_FREQ, &val) != S_OK)
            abort();

        uint64_t now = uclock_now(uclock);
        uint64_t pts = ((upipe_bmd_sink_frame*)completedFrame)->pts;
        upipe_notice_va(upipe, "%p Frame %s (%.2f ms) - delay %.2f ms", completedFrame,
                res[result], dur_to_time(1000 * (val - prev)), dur_to_time(1000 * (now - pts - UCLOCK_FREQ / 25)));
        prev = val;
    }

    virtual HRESULT ScheduledPlaybackHasStopped (void) {
        printf("%s()\n", __func__);
    }

    callback (struct upipe *u, IDeckLinkOutput *out, struct uclock *clock) {
        output = out;
        upipe = u;
        uclock = clock;
        uatomic_store(&refcount, 1);
        prev = 0;
    }

    ~callback(void) {
        uatomic_clean(&refcount);
    }

    virtual ULONG STDMETHODCALLTYPE AddRef(void) {
        return uatomic_fetch_add(&refcount, 1) + 1;
    }

    virtual ULONG STDMETHODCALLTYPE Release(void) {
        uint32_t new_ref = uatomic_fetch_sub(&refcount, 1) - 1;
        if (new_ref == 0)
            delete this;
        return new_ref;
    }

    virtual HRESULT STDMETHODCALLTYPE QueryInterface(REFIID iid, LPVOID *ppv) {
        return E_NOINTERFACE;
    }

private:
    uatomic_uint32_t refcount;
    BMDTimeValue prev;
    IDeckLinkOutput *output;
    struct uclock *uclock;
    struct upipe *upipe;
};

/** @hidden */
static bool upipe_bmd_sink_sub_output(struct upipe *upipe, struct uref *uref,
                                      struct upump **upump_p);

/** @hidden */
static void upipe_bmd_sink_sub_write_watcher(struct upump *upump);

/** @internal @This is the private context of an output of an bmd_sink sink
 * pipe. */
struct upipe_bmd_sink_sub {
    struct urefcount urefcount;

    struct upipe *upipe_bmd_sink;

    /** temporary uref storage */
    struct uchain urefs;
    /** nb urefs in storage */
    unsigned int nb_urefs;
    /** max urefs in storage */
    unsigned int max_urefs;
    /** list of blockers */
    struct uchain blockers;

    /** structure for double-linked lists */
    struct uchain uchain;

    /** delay applied to pts attribute when uclock is provided */
    uint64_t latency;

    /** upump manager */
    struct upump_mgr *upump_mgr;
    /** watcher */
    struct upump *upump;

    /** whether this is an audio pipe */
    bool sound;

    bool s302m;

    /** position in the SDI stream */
    uint8_t channel_idx;

    /** audio buffer to merge tracks */
    int32_t *audio_buf;

    /** public upipe structure */
    struct upipe upipe;
};

/** upipe_bmd_sink structure */
struct upipe_bmd_sink {
    /** refcount management structure */
    struct urefcount urefcount;

    /** manager to create subs */
    struct upipe_mgr sub_mgr;
    /** pic subpipe */
    struct upipe_bmd_sink_sub pic_subpipe;
    /** subpic subpipe */
    struct upipe_bmd_sink_sub subpic_subpipe;

    /** list of input subpipes */
    struct uchain inputs;

    /** card index **/
    int card_idx;

    /** output mode **/
    BMDDisplayMode mode;

    /** video frame index (modulo 5) */
    uint8_t frame_idx;

    /** started flag **/
    int started;
    uint64_t start_pts;
    int preroll;

    /** vanc/vbi temporary buffer **/
    uint16_t vanc_tmp[2][VANC_WIDTH*2];
    uint16_t *dc[2];

    /** closed captioning **/
    uint16_t cdp_hdr_sequence_cntr;

    /** OP47 teletext sequence counter **/
    uint16_t op47_sequence_counter[2];

    /** OP47 total number of teletext packets written **/
    int op47_number_of_packets[2];

    /** vbi **/
    vbi_sampling_par sp;
    vbi_sliced sliced[1];

    /** handle to decklink card */
    IDeckLink *deckLink;
    /** handle to decklink card output */
    IDeckLinkOutput *deckLinkOutput;

    IDeckLinkDisplayMode *displayMode;

    /** hardware uclock */
    struct uclock uclock;

    /** genlock status */
    int genlock_status;

    /** clock offset to ensure it is increasing */
    uint64_t offset;

    /** offset between uclock and pts_prog */
    uint64_t pts_offset;

    /** public upipe structure */
    struct upipe upipe;
};

UPIPE_HELPER_UPIPE(upipe_bmd_sink, upipe, UPIPE_BMD_SINK_SIGNATURE);
UPIPE_HELPER_UREFCOUNT(upipe_bmd_sink, urefcount, upipe_bmd_sink_free);
UPIPE_HELPER_VOID(upipe_bmd_sink);

UPIPE_HELPER_UPIPE(upipe_bmd_sink_sub, upipe, UPIPE_BMD_SINK_INPUT_SIGNATURE)
UPIPE_HELPER_UPUMP_MGR(upipe_bmd_sink_sub, upump_mgr);
UPIPE_HELPER_UPUMP(upipe_bmd_sink_sub, upump, upump_mgr);
UPIPE_HELPER_INPUT(upipe_bmd_sink_sub, urefs, nb_urefs, max_urefs, blockers, upipe_bmd_sink_sub_output);
UPIPE_HELPER_FLOW(upipe_bmd_sink_sub, NULL);
UPIPE_HELPER_SUBPIPE(upipe_bmd_sink, upipe_bmd_sink_sub, input, sub_mgr, inputs, uchain)
UPIPE_HELPER_UREFCOUNT(upipe_bmd_sink_sub, urefcount, upipe_bmd_sink_sub_free);

UBASE_FROM_TO(upipe_bmd_sink, upipe_bmd_sink_sub, pic_subpipe, pic_subpipe)
UBASE_FROM_TO(upipe_bmd_sink, upipe_bmd_sink_sub, subpic_subpipe, subpic_subpipe)

UBASE_FROM_TO(upipe_bmd_sink, uclock, uclock, uclock)

static const bool parity_tab[256] =
{
#   define P2(n) n, n^1, n^1, n
#   define P4(n) P2(n), P2(n^1), P2(n^1), P2(n)
#   define P6(n) P4(n), P4(n^1), P4(n^1), P4(n)
    P6(0), P6(1), P6(1), P6(0)
};

static void upipe_bmd_sink_clear_vbi(uint8_t *dst, int w)
{
    int i;

    for (i = 0; i < w; i++)
        dst[i] = 0x10;

    dst += w;

    for (i = 0; i < w; i++)
        dst[i] = 0x80;
}

static void upipe_bmd_sink_clear_vanc(uint16_t *dst)
{
    int i;

    for (i = 0; i < VANC_WIDTH; i++)
        dst[i] = 0x40;

    dst += VANC_WIDTH;

    for (i = 0; i < VANC_WIDTH; i++)
        dst[i] = 0x200;
}

/* XXX: put this somewhere */
static void upipe_bmd_sink_start_anc(struct upipe *upipe, uint16_t *dst,
                                     int field, uint16_t did, uint16_t sdid)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);

    /* reset dc */
    upipe_bmd_sink->dc[field] = 0;

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
    upipe_bmd_sink->dc[field] = &dst[5];
}

static void upipe_bmd_sink_write_cdp_header(struct upipe *upipe, uint16_t *dst)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);

    /** XXX: Support crazy 25fps captions? **/
    uint8_t fps = upipe_bmd_sink->mode == bmdModeNTSC ||
                  upipe_bmd_sink->mode == bmdModeHD1080i5994 ? 0x4 : 0x7;

    dst[0] = 0x96;
    dst[1] = 0x69;
    dst[2] = 0; // cdp_length
    dst[3] = (fps << 4) | 0xf; // cdp_frame_rate | Reserved
    dst[4] = (1 << 6) | (1 << 1) | 1; // ccdata_present | caption_service_active | Reserved
    dst[5] = upipe_bmd_sink->cdp_hdr_sequence_cntr >> 8;
    dst[6] = upipe_bmd_sink->cdp_hdr_sequence_cntr & 0xff;

    *upipe_bmd_sink->dc[0] += CDP_HEADER_SIZE;
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

    *upipe_bmd_sink->dc[0] += src_size+2;
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

    upipe_bmd_sink->cdp_hdr_sequence_cntr++;

    *upipe_bmd_sink->dc[0] += 4;
    cnt = *upipe_bmd_sink->dc[0];
    upipe_bmd_sink->vanc_tmp[0][ANC_START_LEN+2] = cnt; // set cdp length

    for( i = 0; i < cnt-1; i++ ) // don't include checksum
        checksum += upipe_bmd_sink->vanc_tmp[0][ANC_START_LEN+i];

    dst[3] = checksum ? 256 - checksum : 0;
}

static void upipe_bmd_sink_write_cdp(struct upipe *upipe, const uint8_t *src,
                                     size_t src_size, uint16_t *dst)
{
    upipe_bmd_sink_write_cdp_header(upipe, dst);
    upipe_bmd_sink_write_ccdata_section(upipe, &dst[CDP_HEADER_SIZE], src, src_size);
    upipe_bmd_sink_write_cdp_footer(upipe, &dst[CDP_HEADER_SIZE+src_size+2]);
}

static void upipe_bmd_sink_calc_parity_checksum(struct upipe *upipe, int f2)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);
    uint16_t i;
    uint16_t dc = *upipe_bmd_sink->dc[f2];
    uint16_t checksum = 0;
    uint16_t *buf = upipe_bmd_sink->vanc_tmp[f2];

    /* +3 = did + sdid + dc itself */
    for( i = 0; i < dc+3; i++ )
    {
        uint8_t parity = parity_tab[buf[3+i] & 0xff];
        buf[3+i] |= (!parity << 9) | (parity << 8);

        checksum += buf[3+i] & 0x1ff;
    }

    checksum &= 0x1ff;
    checksum |= (!(checksum >> 8)) << 9;

    buf[ANC_START_LEN+dc] = checksum;
}

static void upipe_bmd_sink_encode_v210(struct upipe *upipe, uint32_t *dst, int field, int vbi)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);
    int width = upipe_bmd_sink->displayMode->GetWidth();
    int w;
    uint32_t val = 0;

    if (vbi) {
        uint8_t *y = (uint8_t*)upipe_bmd_sink->vanc_tmp;
        uint8_t *u = &y[720];

        for( w = 0; w < 720; w += 6 ){
            WRITE_PIXELS8(u, y, (u+1));
            y += 1;
            u += 2;
            WRITE_PIXELS8(y, u, (y+1));
            y += 2;
            u += 1;
            WRITE_PIXELS8(u, y, (u+1));
            y += 1;
            u += 2;
            WRITE_PIXELS8(y, u, (y+1));
            y += 2;
            u += 1;
        }
    } else {
        /* 1280 isn't mod-6 so long vanc packets will be truncated */
        uint16_t *y = &upipe_bmd_sink->vanc_tmp[field][0];
        uint16_t *u = &upipe_bmd_sink->vanc_tmp[field][width];

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

static void upipe_bmd_sink_write_op47_header(struct upipe *upipe, int field)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);

    uint16_t *buf = upipe_bmd_sink->vanc_tmp[field];

    /* Populates upipe_bmd_sink->dc with an address to
     * use later on */
    upipe_bmd_sink_start_anc(upipe, buf, field, 0x43, 0x2);

    /* 2 identifiers */
    buf[ANC_START_LEN]   = 0x51;
    buf[ANC_START_LEN+1] = 0x15;

    /* Length, populate this last */
    buf[ANC_START_LEN+2] = 0x0;

    /* Format code */
    buf[ANC_START_LEN+3] = 0x2;

    /* Data Adaption header */
    buf[ANC_START_LEN+4] = 0x0;
    buf[ANC_START_LEN+5] = 0x0;
    buf[ANC_START_LEN+6] = 0x0;
    buf[ANC_START_LEN+7] = 0x0;
    buf[ANC_START_LEN+8] = 0x0;
}

static void upipe_bmd_sink_write_op47_packet(struct upipe *upipe, const uint8_t *data,
                                             uint8_t line_offset, uint8_t f2)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);

    uint16_t *buf = upipe_bmd_sink->vanc_tmp[f2];

    /* Write structure A */
    int num_packets = upipe_bmd_sink->op47_number_of_packets[f2];
    buf[ANC_START_LEN + OP47_INITIAL_WORDS + num_packets]  = ((!f2) << 7) | line_offset;

    /* Structure B */
    int idx = OP47_STRUCT_B_OFFSET + 45*num_packets;

    /* 2x Run in codes */
    buf[idx++] = 0x55;
    buf[idx++] = 0x55;

    /* Framing code, MRAG and the data */
    for (int i = 1; i < 44; i++)
        buf[idx++] = REVERSE( data[i] );

    upipe_bmd_sink->op47_number_of_packets[f2]++;
}

static void upipe_bmd_sink_write_op47_footer(struct upipe *upipe, int f2)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);

    uint16_t *buf = upipe_bmd_sink->vanc_tmp[f2];
    int idx = OP47_STRUCT_B_OFFSET + 45*upipe_bmd_sink->op47_number_of_packets[f2];

    /* Footer ID */
    buf[idx++] = 0x74;

    /* Sequence counter, MSB and LSB */
    upipe_bmd_sink->op47_sequence_counter[f2]++;
    buf[idx++] = (upipe_bmd_sink->op47_sequence_counter[f2] >> 8) & 0xff;
    buf[idx++] = (upipe_bmd_sink->op47_sequence_counter[f2] >> 0) & 0xff;

    /* Write UDW length (includes checksum so do it before) */
    buf[ANC_START_LEN+2] = idx + 1 - ANC_START_LEN;

    /* SDP Checksum */
    uint8_t checksum = 0;
    for (int i = ANC_START_LEN; i < idx; i++)
        checksum += buf[i];
    buf[idx++] = checksum ? 256 - checksum : 0;

    /* Write ADF DC */
    *upipe_bmd_sink->dc[f2] = idx - ANC_START_LEN;
}

/* VBI Teletext */
static void upipe_bmd_sink_extract_ttx(struct upipe *upipe, IDeckLinkVideoFrameAncillary *ancillary,
                                       const uint8_t *pic_data, size_t pic_data_size, int sd)
{
    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);
    void *vanc[2];
    pic_data++;
    pic_data_size--;

    if(!sd) {
        ancillary->GetBufferForVerticalBlankingLine(OP47_LINE1, &vanc[0]);
        upipe_bmd_sink_clear_vanc(upipe_bmd_sink->vanc_tmp[0]);

        ancillary->GetBufferForVerticalBlankingLine(OP47_LINE2, &vanc[1]);
        upipe_bmd_sink_clear_vanc(upipe_bmd_sink->vanc_tmp[1]);

        upipe_bmd_sink->op47_number_of_packets[0] = upipe_bmd_sink->op47_number_of_packets[1] = 0;
    }

    while (pic_data_size >= 46)
    {
        uint8_t data_unit_id = pic_data[0];
        if (data_unit_id == 0x2 || data_unit_id == 0x3) {
            uint8_t data_unit_len = pic_data[1];
            if (data_unit_len == 0x2c) {
                uint8_t line_offset = pic_data[2] & 0x1f;
                uint8_t f2 = (pic_data[2] >> 5) & 1;
                uint16_t line = line_offset + (PAL_FIELD_OFFSET * f2);
                if (line > 0) {
                    /* Setup libzvbi or OP-47 */
                    if (sd) {
                        uint8_t *buf = (uint8_t*)upipe_bmd_sink->vanc_tmp[0];
                        upipe_bmd_sink_clear_vbi(buf, 720);

                        upipe_bmd_sink->sp.start[f2] = line;
                        upipe_bmd_sink->sp.count[f2] = 1;
                        upipe_bmd_sink->sp.count[!f2] = 0;

                        upipe_bmd_sink->sliced[0].id = VBI_SLICED_TELETEXT_B;
                        upipe_bmd_sink->sliced[0].line = line;
                        for (int i = 0; i < 42; i++)
                            upipe_bmd_sink->sliced[0].data[i] = REVERSE(pic_data[4+i]);

                        int success = vbi_raw_video_image(buf, 720, &upipe_bmd_sink->sp,
                                                          0, 0, 0, 0x000000FF, false,
                                                          upipe_bmd_sink->sliced, 1);

                        ancillary->GetBufferForVerticalBlankingLine(line, &vanc[0]);
                        upipe_bmd_sink_encode_v210(upipe, (uint32_t*)vanc[0], 0, 1);
                    } else {
                        if (!upipe_bmd_sink->op47_number_of_packets[f2])
                            upipe_bmd_sink_write_op47_header(upipe, f2);
                        if (upipe_bmd_sink->op47_number_of_packets[f2] < 5)
                            upipe_bmd_sink_write_op47_packet(upipe, &pic_data[2], line_offset, f2);
                    }
                }
            }
        }

        pic_data += 46;
        pic_data_size -= 46;
    }

    if (!sd) {
        for (int i = 0; i < 2; i++) {
            if (upipe_bmd_sink->op47_number_of_packets[i]) {
                upipe_bmd_sink_write_op47_footer(upipe, i);
                upipe_bmd_sink_calc_parity_checksum(upipe, i);
                upipe_bmd_sink_encode_v210(upipe, (uint32_t*)vanc[i], i, 0);
            }
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
        struct upipe_mgr *sub_mgr, struct uprobe *uprobe, bool static_pipe)
{
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_sub_mgr(sub_mgr);

    if (static_pipe) {
        upipe_init(upipe, sub_mgr, uprobe);
        /* increment super pipe refcount only when the static pipes are retrieved */
        upipe_mgr_release(sub_mgr);
        upipe->refcount = &upipe_bmd_sink->urefcount;
    } else
        upipe_bmd_sink_sub_init_urefcount(upipe);

    struct upipe_bmd_sink_sub *upipe_bmd_sink_sub = upipe_bmd_sink_sub_from_upipe(upipe);
    upipe_bmd_sink_sub->upipe_bmd_sink = upipe_bmd_sink_to_upipe(upipe_bmd_sink);

    upipe_bmd_sink_sub_init_sub(upipe);
    upipe_bmd_sink_sub_init_input(upipe);
    upipe_bmd_sink_sub_init_upump_mgr(upipe);
    upipe_bmd_sink_sub_init_upump(upipe);
    upipe_bmd_sink_sub->sound = !static_pipe;

    upipe_throw_ready(upipe);
}

static void upipe_bmd_sink_sub_free(struct upipe *upipe)
{
    struct upipe_bmd_sink_sub *upipe_bmd_sink_sub = upipe_bmd_sink_sub_from_upipe(upipe);
    upipe_throw_dead(upipe);
    upipe_bmd_sink_sub_clean_sub(upipe);
    upipe_bmd_sink_sub_clean_upump(upipe);
    upipe_bmd_sink_sub_clean_upump_mgr(upipe);
    upipe_bmd_sink_sub_clean_input(upipe);

    struct upipe_bmd_sink *upipe_bmd_sink =
        upipe_bmd_sink_from_sub_mgr(upipe->mgr);
    if (upipe_bmd_sink_sub == &upipe_bmd_sink->subpic_subpipe ||
        upipe_bmd_sink_sub == &upipe_bmd_sink->pic_subpipe) {
        upipe_clean(upipe);
        return;
    }

    upipe_bmd_sink_sub_clean_urefcount(upipe);
    upipe_bmd_sink_sub_free_flow(upipe);
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

static int upipe_bmd_sink_sub_read_uref_attributes(struct uref *uref,
    uint64_t *pts, struct urational *drift_rate, size_t *size)
{
    UBASE_RETURN(uref_clock_get_pts_sys(uref, pts));
    UBASE_RETURN(uref_clock_get_rate(uref, drift_rate));
    UBASE_RETURN(uref_sound_size(uref, size, NULL /* sample_size */));

    if (drift_rate->num == 0 || drift_rate->den == 0) {
        drift_rate->num = 1;
        drift_rate->den = 1;
    }

    return UBASE_ERR_NONE;
}

static void start_code(const int32_t *buf, uint8_t channel_idx, unsigned samples)
{
    bool sync = false;
    bool zero = true;

    for (size_t i = 0; i < 1920; i++) {
        if (buf[DECKLINK_CHANNELS*i + channel_idx] != 0 ||
                buf[DECKLINK_CHANNELS*i + channel_idx+1] != 0)
            zero = false;

        if (!sync && buf[DECKLINK_CHANNELS*i + channel_idx +1] == 0x6f872000) {
            printf("[%d] MISSED SYNC on 2nd sample (off %zu/1920)\n", channel_idx/2, i);
        }

        if (buf[DECKLINK_CHANNELS*i + channel_idx] != 0x6f872000)
            continue;

        if (i+4 > 1920) {
            printf("start code too late (at sample %zu)\n", i);
            return;
        }

        if (buf[DECKLINK_CHANNELS*i+channel_idx+1] != 0x54e1f000) {
            printf("found Pa but not Pb\n");
            continue;
        }

        if (buf[DECKLINK_CHANNELS*(i+1)+channel_idx] != 0x003c0000) {
            printf("Unexpected Pc 0x%x\n", buf[DECKLINK_CHANNELS*(i+1)+channel_idx]);
            continue;
        }

        if (buf[DECKLINK_CHANNELS*(i+1)+channel_idx+1] != 0x11d00000) {
            printf("Unexpected Pd 0x%x\n", buf[DECKLINK_CHANNELS*(i+1)+channel_idx+1]);
            continue;
        }

        if (buf[DECKLINK_CHANNELS*(i+2)+channel_idx] != 0x0788e000) {
            printf("Unexpected Dolby-E start code 0x%x\n", buf[DECKLINK_CHANNELS*(i+2)+channel_idx]);
            continue;
        }

        const unsigned frame_size_samples = 9120 /* bytes */ / 5 /* bytes per samples pair */;

        //if (i != 216)
        printf("[%d] S337 sync at sample %zu/1920 : burst %zu -> %zu\n", channel_idx / 2,
                i, i + 4, i + 4 + frame_size_samples);
        sync = true;
    }

    if (!sync && !zero)
        printf("[%d] NO SYNC and frame not all zeros\n", channel_idx/2);
}

static void copy_samples(int32_t *out, uint8_t idx, struct uref *uref, uint64_t offset, uint64_t samples)
{
    const int32_t *in;
    uref_sound_read_int32_t(uref, 0, samples, &in, 1);
    for (int i = 0; i < samples; i++)
        memcpy(&out[DECKLINK_CHANNELS * (offset + i) + idx], &in[2*i], 2 * sizeof(int32_t));

    uref_sound_unmap(uref, 0, samples, 1);
}

static inline uint64_t length_to_samples(const uint64_t length)
{
    /* rounding down */
    return (length * 48000) / UCLOCK_FREQ;
}

/** @internal @This fills the audio samples for one single stereo pair
 */
static void upipe_bmd_sink_sub_sound_get_samples_channel(struct upipe *upipe,
        const uint64_t video_pts, const unsigned samples,
        struct upipe_bmd_sink_sub *upipe_bmd_sink_sub)
{
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_upipe(upipe);

    /* timestamp of next video frame */
    const uint64_t last_pts = video_pts + samples * UCLOCK_FREQ / 48000;

    uint64_t start_offset = UINT64_MAX;
    uint64_t end_offset = 0;

    if (0) upipe_dbg_va(upipe, "\tChannel %hu - video pts %f (%"PRIu64")",
            upipe_bmd_sink_sub->channel_idx/2, pts_to_time(video_pts), video_pts);

    uint64_t old_pts = 0; // debug

    /* iterate through subpipe queue */
    struct uchain *uchain, *uchain_tmp;
    ulist_delete_foreach(&upipe_bmd_sink_sub->urefs, uchain, uchain_tmp) {
        struct uref *uref = uref_from_uchain(uchain);

        size_t uref_samples = 0;                /* samples in the uref */
        struct urational drift_rate = { 0, 0 }; /* playing rate */
        uint64_t duration;                      /* uref real duration */
        uint64_t pts = UINT64_MAX;              /* presentation timestamp */
        uint64_t samples_offset;                /* samples already written */
        uint64_t missing_samples;               /* samples yet to be read */
        int64_t time_offset;                    /* delay between uref start and video frame */

        /* read uref attributes */
        if (!ubase_check(upipe_bmd_sink_sub_read_uref_attributes(uref,
            &pts, &drift_rate, &uref_samples))) {
            upipe_err(upipe, "Could not read uref attributes");
            goto drop_uref;
        }

        pts += upipe_bmd_sink_sub->latency;

        /* samples / sample rate = duration */
        duration = uref_samples * UCLOCK_FREQ / 48000;

        /* multiply uref duration by its playing rate to get its real duration */
        duration *= drift_rate.num;
        duration /= drift_rate.den;

        time_offset = pts - video_pts;

        if (!old_pts)
            old_pts = pts;
        if (0) upipe_dbg_va(upipe, "uref pts %f (%"PRIu64", +%"PRIu64") duration %"PRIu64" offset %"PRId64,
                pts_to_time(pts), pts, pts - old_pts, duration, time_offset);
        old_pts = pts;

        /* likely to happen when starting but not after */
        if (unlikely(time_offset < 0)) {
            /* audio PTS is earlier than video PTS */

            /* duration of audio to discard */
            uint64_t drop_duration = -time_offset;

            /* too late */
            if (unlikely(duration < drop_duration)) {
                upipe_err_va(upipe, "TOO LATE by %"PRIu64" ticks, dropping %zu samples (%f + %f < %f)",
                        video_pts - pts - duration,
                        uref_samples,
                        pts_to_time(pts), dur_to_time(duration), pts_to_time(video_pts)
                        );

                goto drop_uref;
            }

            /* drop beginning of uref */
            size_t drop_samples = length_to_samples(drop_duration);
            if (drop_samples > uref_samples)
                drop_samples = uref_samples;

            if (drop_samples) {
                upipe_dbg_va(upipe, "[%d] DROPPING %zu samples for PTS %f / %"PRIu64" ticks (%f)",
                        upipe_bmd_sink_sub->channel_idx/2,
                        drop_samples, pts_to_time(pts), drop_duration, dur_to_time(drop_duration));

                /* resize buffer */
                uref_sound_resize(uref, drop_samples, -1);
            }

            pts = video_pts;
            time_offset = 0;
            duration -= drop_duration;
            uref_samples -= drop_samples;

            uref_clock_set_pts_sys(uref, pts);
        } else if (unlikely(pts > last_pts)) { /* too far in the future ? */
            upipe_err_va(upipe, "TOO EARLY (%f > %f) by %fs (%"PRIu64" ticks)",
                    pts_to_time(pts), pts_to_time(last_pts),
                    dur_to_time(pts - last_pts), pts - last_pts
                    );
            upipe_dbg_va(upipe, "\t\tStart %u End %u Samples %"PRIu64, start_offset, end_offset, samples_offset);
            break;
        }

        /* writing position in the outgoing block */
        samples_offset = length_to_samples(time_offset);
        /* we can't write past the end of the buffer */
        if (samples_offset > samples - 1) {
            upipe_err_va(upipe, "FIXING offset: %"PRIu64" > %u",
                samples_offset, samples - 1);
            samples_offset = samples - 1;
        }

        // assert(samples_offset == end_offset);
        if (samples_offset < end_offset) {
            if (samples_offset == end_offset - 1)
                samples_offset = end_offset; // TODO : fix timestamps
            else
                upipe_err_va(upipe, "Mismatching offsets: %"PRIu64" != %u", samples_offset, end_offset);
        }

        /* The earliest in the outgoing block we've written to */
        if (start_offset > samples_offset)
            start_offset = samples_offset;

        /* how many samples we want to read */
        // XXX : rounding here
        missing_samples = length_to_samples(last_pts - pts);

        if (missing_samples == samples - samples_offset - 1) {
            missing_samples = samples - samples_offset;
            // XXX
        }

        /* we can't read more samples than available in our buffer */
        if (missing_samples > samples - samples_offset)
            missing_samples = samples - samples_offset;

        /* is our uref too small ? */
        if (missing_samples > uref_samples)
            missing_samples = uref_samples;

        if (0) upipe_dbg_va(upipe, "reading %u samples", missing_samples);

        /* read the samples into our final buffer */
        copy_samples(upipe_bmd_sink->pic_subpipe.audio_buf,
                upipe_bmd_sink_sub->channel_idx, uref,
                samples_offset, missing_samples);

        if (upipe_bmd_sink_sub->s302m)
            start_code(upipe_bmd_sink->pic_subpipe.audio_buf, upipe_bmd_sink_sub->channel_idx,
                    samples);

        /* The latest in the outgoing block we've written to */
        if (end_offset < samples_offset + missing_samples)
            end_offset = samples_offset + missing_samples;

        if (uref_samples - missing_samples > 0) {
            /* we did not exhaust this uref, resize it and we're done */
            pts -= upipe_bmd_sink_sub->latency;
            pts += missing_samples * UCLOCK_FREQ / 48000;
            uref_clock_set_pts_sys(uref, pts);
            uref_sound_resize(uref, missing_samples, -1);
            //assert(end_offset == samples);
            break;
        }

drop_uref:
        ulist_delete(uchain);
        upipe_bmd_sink_sub->nb_urefs--;
        uref_free(uref);

        if (end_offset == samples)
            break;
    }

    if (start_offset == UINT64_MAX) {
        //assert(upipe_bmd_sink_sub->nb_urefs == 0);
        if (upipe_bmd_sink_sub->nb_urefs != 0)
            upipe_err_va(upipe, "COULD NOT START DESPITE %u buffered urefs", upipe_bmd_sink_sub->nb_urefs);
        upipe_err_va(upipe, "NO BUFFERS for vid PTS %f", pts_to_time(video_pts));
        return;
    }

    if (start_offset > 0) {
        upipe_err_va(upipe, "Start offset %"PRIu64, start_offset);
        // TODO : fix hole
    }

    if (end_offset < samples) {
        //assert(upipe_bmd_sink_sub->nb_urefs == 0);
        upipe_err_va(upipe, "[%d] MISSED %"PRIu64" samples, last pts %f (%u urefs buffered)",
                upipe_bmd_sink_sub->channel_idx/2,
                samples - end_offset, pts_to_time(last_pts), upipe_bmd_sink_sub->nb_urefs);
        // TODO : fix hole
    }
}

/** @internal @This fills one video frame worth of audio samples
 */
static void upipe_bmd_sink_sub_sound_get_samples(struct upipe *upipe,
        const uint64_t video_pts, const unsigned samples)
{
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_upipe(upipe);

    /* Clear buffer */
    memset(upipe_bmd_sink->pic_subpipe.audio_buf, 0,
            samples * DECKLINK_CHANNELS * sizeof(int32_t));

    /* interate through input subpipes */
    struct uchain *uchain;
    ulist_foreach(&upipe_bmd_sink->inputs, uchain) {
        struct upipe_bmd_sink_sub *upipe_bmd_sink_sub =
            upipe_bmd_sink_sub_from_uchain(uchain);

        if (upipe_bmd_sink_sub->sound)
            upipe_bmd_sink_sub_sound_get_samples_channel(upipe, video_pts, samples, upipe_bmd_sink_sub);
    }
}

static inline unsigned audio_samples_count(struct upipe_bmd_sink *upipe_bmd_sink,
        BMDTimeValue timeValue, BMDTimeScale timeScale)
{
    const unsigned samples = (uint64_t)48000 * timeValue / timeScale;

    /* fixed number of samples for 48kHz */
    if (timeValue != 1001 || timeScale == 24000)
        return samples;

    if (unlikely(timeScale != 30000 && timeScale != 60000)) {
        upipe_err_va(&upipe_bmd_sink->upipe,
                "Unsupported rate %"PRIu64"/%"PRIu64, timeScale, timeValue);
        return samples;
    }

    /* cyclic loop of 5 different sample counts */
    if (++upipe_bmd_sink->frame_idx == 5)
        upipe_bmd_sink->frame_idx = 0;

    static const uint8_t samples_increment[2][5] = {
        { 1, 0, 1, 0, 1 }, /* 30000 / 1001 */
        { 1, 1, 1, 1, 0 }  /* 60000 / 1001 */
    };

    bool rate5994 = !!(timeScale == 60000);

    return samples + samples_increment[rate5994][upipe_bmd_sink->frame_idx];
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

    const char *def;
    if (unlikely(ubase_check(uref_flow_get_def(uref, &def)))) {
        upipe_bmd_sink_sub->latency = 0;

        uref_clock_get_latency(uref, &upipe_bmd_sink_sub->latency);
        upipe_dbg_va(upipe, "latency %"PRIu64, upipe_bmd_sink_sub->latency);
        uint8_t data_type = 0;
        uref_attr_get_small_unsigned(uref, &data_type, UDICT_TYPE_SMALL_UNSIGNED, "data_type");
        upipe_bmd_sink_sub->s302m = data_type == 28;

#if 0
        const char *codec = NULL;
        uref_attr_get_string(uref, &codec, UDICT_TYPE_STRING, "codec");
        upipe_bmd_sink_sub->s302m = codec && !strcmp(codec, "s302m");
#endif

        upipe_bmd_sink_sub_check_upump_mgr(upipe);

        uref_free(uref);
        return true;
    }

    if (upipe_bmd_sink_sub->sound
        || upipe_bmd_sink_sub == &upipe_bmd_sink->subpic_subpipe)
        return false;

    /* pic subpipe */

    uint64_t pts = 0;
    if (likely(ubase_check(uref_clock_get_pts_sys(uref, &pts)))) {
#if 0   /* workaround for files */
        uint64_t pts_prog = 0;
        uref_clock_get_pts_prog(uref, &pts_prog);
        if (upipe_bmd_sink->pts_offset == 0) {
            upipe_bmd_sink->pts_offset = pts_prog - pts;
        } else
            pts = pts_prog - upipe_bmd_sink->pts_offset;
#endif

        uint64_t now = uclock_now(&upipe_bmd_sink->uclock);
        pts += upipe_bmd_sink_sub->latency;

        uint64_t target = pts;
        if (1 || upipe_bmd_sink->started) {
            target += PREROLL_FRAMES * UCLOCK_FREQ / 25; // preroll

            if (now < target) {
                upipe_dbg_va(upipe, "sleeping %"PRIu64" (%"PRIu64")",
                        target - now, target);
                upipe_bmd_sink_sub_wait_upump(upipe, target - now,
                        upipe_bmd_sink_sub_write_watcher);
                return false;
            } else if (now > target + UCLOCK_FREQ / 10) {
                upipe_warn_va(upipe, "late uref dropped (%"PRId64")",
                        (now - target) * 1000 / UCLOCK_FREQ);
                uref_free(uref);
                return true;
            }
        }
    }

    int w = upipe_bmd_sink->displayMode->GetWidth();
    int h = upipe_bmd_sink->displayMode->GetHeight();
    int sd = upipe_bmd_sink->mode == bmdModePAL || upipe_bmd_sink->mode == bmdModeNTSC;
    int ttx = upipe_bmd_sink->mode == bmdModePAL || upipe_bmd_sink->mode == bmdModeHD1080i50;
    const uint8_t *pic_data = NULL;
    size_t pic_data_size = 0;

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
                                                                 w, h, pts);
    if (!video_frame) {
        uref_free(uref);
        return true;
    }

    BMDTimeValue timeValue;
    BMDTimeScale timeScale;
    upipe_bmd_sink->displayMode->GetFrameRate(&timeValue, &timeScale);

    IDeckLinkVideoFrameAncillary *ancillary = NULL;
    upipe_bmd_sink->deckLinkOutput->CreateAncillaryData(video_frame->GetPixelFormat(), &ancillary);

    uref_pic_get_cea_708(uref, &pic_data, &pic_data_size);
    if( pic_data_size > 0 )
    {
        void *vanc;
        ancillary->GetBufferForVerticalBlankingLine(CC_LINE, &vanc);
        upipe_bmd_sink_clear_vanc(upipe_bmd_sink->vanc_tmp[0]);
        upipe_bmd_sink_start_anc(upipe, upipe_bmd_sink->vanc_tmp[0], 0, 0x61, 0x1);
        upipe_bmd_sink_write_cdp(upipe, pic_data, pic_data_size, &upipe_bmd_sink->vanc_tmp[0][ANC_START_LEN]);
        upipe_bmd_sink_calc_parity_checksum(upipe, 0);

        upipe_bmd_sink_encode_v210(upipe, (uint32_t*)vanc, 0, sd);
    }

    /* Loop through subpic data */
    //printf("\n subpic depth %i \n", upipe_bmd_sink_sub->nb_urefs);
    struct uchain *uchain, *uchain_tmp;
    ulist_delete_foreach(&upipe_bmd_sink_sub->urefs, uchain, uchain_tmp) {
        //printf("\n VIDEO PTS %"PRIu64" \n", pts );
        struct uref *uref = uref_from_uchain(uchain);
        uint64_t subpic_pts = 0;
        uref_clock_get_pts_sys(uref, &subpic_pts);
        subpic_pts += upipe_bmd_sink->subpic_subpipe.latency;
        //printf("\n SUBPIC PTS %"PRIu64" \n", subpic_pts );

        /* Delete old urefs */
        if (subpic_pts + (UCLOCK_FREQ/25) < pts) {
            ulist_delete(uchain);
            upipe_bmd_sink_sub->nb_urefs--;
            uref_free(uref);
            continue;
        }

        /* Choose the closest subpic in the past */
        if (ttx && pts - (UCLOCK_FREQ/25) < subpic_pts) {
            //printf("\n CHOSEN SUBPIC %"PRIu64" \n", subpic_pts);
            const uint8_t *buf;
            int size = -1;
            if (ubase_check(uref_block_read(uref, 0, &size, &buf))) {
                upipe_bmd_sink_extract_ttx(upipe, ancillary, buf, size, sd);
                uref_block_unmap(uref, 0);
            }
            ulist_delete(uchain);
            upipe_bmd_sink_sub->nb_urefs--;
            uref_free(uref);
            break;
        }
    }

    video_frame->SetAncillaryData(ancillary);

    const unsigned samples = audio_samples_count(upipe_bmd_sink, timeValue, timeScale);
    upipe_bmd_sink_sub_sound_get_samples(&upipe_bmd_sink->upipe, pts, samples);
    HRESULT result;

    result = upipe_bmd_sink->deckLinkOutput->ScheduleVideoFrame(video_frame, pts, UCLOCK_FREQ * timeValue / timeScale, UCLOCK_FREQ);
    if( result != S_OK )
        upipe_err_va(upipe, "DROPPED FRAME %x", result);

    if (!upipe_bmd_sink->started) {
        result = upipe_bmd_sink->deckLinkOutput->BeginAudioPreroll();
        if (result != S_OK)
            upipe_err_va(upipe, "Begin preroll failed: %lx", result);
    }
    uint32_t written;
    result = upipe_bmd_sink->deckLinkOutput->ScheduleAudioSamples(
            upipe_bmd_sink->pic_subpipe.audio_buf, samples, pts,
            UCLOCK_FREQ, &written);
    if( result != S_OK ) {
        upipe_err_va(upipe, "DROPPED AUDIO: %lx", result);
        written = 0;
    }

    if (upipe_bmd_sink->start_pts == 0)
        upipe_bmd_sink->start_pts = pts;

    if (!upipe_bmd_sink->started) {
        if (--upipe_bmd_sink->preroll == 0) {
            upipe_notice(upipe, "Starting playback");
            result = upipe_bmd_sink->deckLinkOutput->EndAudioPreroll();
            if (result != S_OK)
                upipe_err_va(upipe, "End preroll failed: %lx", result);
            upipe_bmd_sink->deckLinkOutput->StartScheduledPlayback(upipe_bmd_sink->start_pts, UCLOCK_FREQ, 1.0);
            upipe_bmd_sink->started = 1;
        }
    }

    uint32_t buffered;
    upipe_bmd_sink->deckLinkOutput->GetBufferedAudioSampleFrameCount(&buffered);

    if (0) if (buffered == 0) {
        /* TODO: get notified as soon as audio buffers empty */
        upipe_bmd_sink->deckLinkOutput->StopScheduledPlayback(0, NULL, 0);
        upipe_bmd_sink->started = 0;
    }
    if (written != samples)
        upipe_dbg_va(upipe, "written %u/%u", written, samples);

    uint32_t vbuffered;
    upipe_bmd_sink->deckLinkOutput->GetBufferedVideoFrameCount(&vbuffered);
    upipe_dbg_va(upipe, "A %.2f | V %.2f",
            (float)(1000 * buffered) / 48000, (float) 1000 * vbuffered / 25);

    bool active;
    if (upipe_bmd_sink->deckLinkOutput->IsScheduledPlaybackRunning(&active) != S_OK)
        abort();
    //assert(active);

    video_frame->Release();

    int genlock_status;
    upipe_bmd_sink_get_genlock_status(&upipe_bmd_sink->upipe, &genlock_status);
    if (upipe_bmd_sink->genlock_status == UPIPE_BMD_SINK_GENLOCK_UNLOCKED) {
        if (genlock_status == UPIPE_BMD_SINK_GENLOCK_LOCKED) {
            upipe_warn(upipe, "genlock synchronized, restarting");
            upipe_bmd_sink->deckLinkOutput->StopScheduledPlayback(0, NULL, 0);
            upipe_bmd_sink->deckLinkOutput->StartScheduledPlayback(pts, UCLOCK_FREQ, 1.0);
        }
    }

    upipe_bmd_sink->genlock_status = genlock_status;

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
        struct upipe_bmd_sink_sub *upipe_bmd_sink_sub =
            upipe_bmd_sink_sub_from_upipe(upipe);
        upipe_bmd_sink_sub_hold_input(upipe, uref);
        if (!upipe_bmd_sink_sub->sound)
            upipe_bmd_sink_sub_block_input(upipe, upump_p);
    }
}

uint32_t upipe_bmd_mode_from_flow_def(struct upipe *upipe, struct uref *flow_def)
{
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_upipe(upipe);
    IDeckLinkOutput *deckLinkOutput = upipe_bmd_sink->deckLinkOutput;
    char *displayModeName = NULL;
    uint32_t bmdMode = bmdModeUnknown;

    if (!deckLinkOutput) {
        upipe_err(upipe, "Card not opened yet");
        return bmdModeUnknown;
    }

    uint64_t hsize, vsize;
    struct urational fps;
    if (unlikely(!ubase_check(uref_pic_flow_get_hsize(flow_def, &hsize)) ||
                !ubase_check(uref_pic_flow_get_vsize(flow_def, &vsize)) ||
                !ubase_check(uref_pic_flow_get_fps(flow_def, &fps)))) {
        upipe_err(upipe, "cannot read size and frame rate");
        uref_dump(flow_def, upipe->uprobe);
        return bmdModeUnknown;
    }

    bool interlaced = !ubase_check(uref_pic_get_progressive(flow_def));

    upipe_notice_va(upipe, "%"PRIu64"x%"PRIu64" %"PRId64"/%"PRIu64" interlaced %d",
            hsize, vsize, fps.num, fps.den, interlaced);

    IDeckLinkDisplayModeIterator *displayModeIterator = NULL;
    HRESULT result = deckLinkOutput->GetDisplayModeIterator(&displayModeIterator);
    if (result != S_OK){
        upipe_err(upipe, "decklink card has no display modes");
        return bmdModeUnknown;
    }

    IDeckLinkDisplayMode *mode = NULL;
    while ((result = displayModeIterator->Next(&mode)) == S_OK) {
        BMDFieldDominance field;
        BMDTimeValue timeValue;
        BMDTimeScale timeScale;
        struct urational bmd_fps;

        if (mode->GetWidth() != hsize)
            goto next;

        if (mode->GetHeight() != vsize)
            goto next;

        mode->GetFrameRate(&timeValue, &timeScale);
        bmd_fps.num = timeScale;
        bmd_fps.den = timeValue;

        if (urational_cmp(&fps, &bmd_fps))
            goto next;

        field = mode->GetFieldDominance();
        if (field == bmdUnknownFieldDominance) {
            upipe_err(upipe, "unknown field dominance");
        } else if (field == bmdLowerFieldFirst || field == bmdUpperFieldFirst) {
            if (!interlaced) {
                goto next;
            }
        } else {
            if (interlaced) {
                goto next;
            }
        }
        break;
next:
        mode->Release();
    }

    if (result != S_OK || !mode)
        goto end;

    if (mode->GetName((const char**)&displayModeName) == S_OK) {
        upipe_dbg_va(upipe, "Flow def is mode %s", displayModeName);
        free(displayModeName);
    }
    bmdMode = mode->GetDisplayMode();

end:
    if (mode)
        mode->Release();

    displayModeIterator->Release();

    return bmdMode;
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

    uint64_t latency;
    if (ubase_check(uref_clock_get_latency(flow_def, &latency))) {
        if (/*upipe_bmd_sink_sub->latency && */latency != upipe_bmd_sink_sub->latency) {
            upipe_dbg_va(upipe, "latency %"PRIu64" -> %"PRIu64,
                    upipe_bmd_sink_sub->latency, latency);
            upipe_bmd_sink_sub->latency = latency;
        }
    }

    if (upipe_bmd_sink_sub == &upipe_bmd_sink->pic_subpipe) {
        uint8_t macropixel;
        if (!ubase_check(uref_pic_flow_get_macropixel(flow_def, &macropixel))) {
            upipe_err(upipe, "macropixel size not set");
            uref_dump(flow_def, upipe->uprobe);
            return UBASE_ERR_EXTERNAL;
        }

        BMDDisplayMode bmdMode = upipe_bmd_mode_from_flow_def(&upipe_bmd_sink->upipe, flow_def);
        if (bmdMode != upipe_bmd_sink->mode) {
            upipe_err(upipe, "Flow def doesn't correspond to configured mode");
            return UBASE_ERR_UNHANDLED;
        }

        if (macropixel != 48 || ubase_check(
                             uref_pic_flow_check_chroma(flow_def, 1, 1, 1,
                                                        "u10y10v10y10u10y10v10y10u10y10v10y10"))) {
            upipe_err(upipe, "incompatible input flow def");
            uref_dump(flow_def, upipe->uprobe);
            return UBASE_ERR_EXTERNAL;
        }
        upipe_bmd_sink->frame_idx = 0;
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

static struct upipe *upipe_bmd_sink_sub_alloc(struct upipe_mgr *mgr,
                                                 struct uprobe *uprobe,
                                                 uint32_t signature, va_list args)
{
    struct uref *flow_def;
    struct upipe *upipe = upipe_bmd_sink_sub_alloc_flow(mgr,
            uprobe, signature, args, &flow_def);
    struct upipe_bmd_sink_sub *upipe_bmd_sink_sub;

    if (unlikely(upipe == NULL || flow_def == NULL))
        goto error;

    const char *def;
    if (!ubase_check(uref_flow_get_def(flow_def, &def)))
        goto error;

    if (ubase_ncmp(def, "sound."))
        goto error;

    uint8_t channel_idx;
    if (!ubase_check(uref_attr_get_small_unsigned(flow_def, &channel_idx,
            UDICT_TYPE_SMALL_UNSIGNED, "channel_idx"))) {
        upipe_err(upipe, "Could not read channel_idx");
        uref_dump(flow_def, uprobe);
        goto error;
    }

    if (channel_idx >= DECKLINK_CHANNELS) {
        upipe_err_va(upipe, "channel_idx %hu not in range", channel_idx);
        goto error;
    }

    upipe_bmd_sink_sub_init(upipe, mgr, uprobe, false);

    upipe_bmd_sink_sub = upipe_bmd_sink_sub_from_upipe(upipe);
    upipe_bmd_sink_sub->channel_idx = channel_idx;

    /* different subpipe type */
    uref_dump(flow_def, uprobe);
    uref_free(flow_def);

    return upipe;

error:
    uref_free(flow_def);
    upipe_release(upipe);
    return NULL;
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
    sub_mgr->upipe_alloc = upipe_bmd_sink_sub_alloc;
    sub_mgr->upipe_input = upipe_bmd_sink_sub_input;
    sub_mgr->upipe_control = upipe_bmd_sink_sub_control;
    sub_mgr->upipe_mgr_control = NULL;
}

/** @This returns the Blackmagic hardware output time.
 *
 * @param uclock utility structure passed to the module
 * @return current hardware output time in 27 MHz ticks
 */
static uint64_t uclock_bmd_sink_now(struct uclock *uclock)
{
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_uclock(uclock);
    struct upipe *upipe = &upipe_bmd_sink->upipe;

    BMDTimeValue hardware_time = UINT64_MAX, time_in_frame, ticks_per_frame;

    if (!upipe_bmd_sink->deckLinkOutput) {
        upipe_err_va(upipe, "No output configured");
        return UINT64_MAX;
    }


    HRESULT res = upipe_bmd_sink->deckLinkOutput->GetHardwareReferenceClock(
            UCLOCK_FREQ, &hardware_time, &time_in_frame, &ticks_per_frame);
    if (res != S_OK) {
        upipe_err_va(upipe, "\t\tCouldn't read hardware clock: 0x%08lx", res);
        hardware_time = 0;
    }

    hardware_time += upipe_bmd_sink->offset;

    if (0) upipe_notice_va(upipe, "CLOCK THR 0x%llx VAL %"PRIu64,
        (unsigned long long)pthread_self(), (uint64_t)hardware_time);

    return (uint64_t)hardware_time;
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
    struct uprobe *uprobe_subpic = va_arg(args, struct uprobe *);

    struct upipe_bmd_sink *upipe_bmd_sink =
        (struct upipe_bmd_sink *)calloc(1, sizeof(struct upipe_bmd_sink));
    if (unlikely(upipe_bmd_sink == NULL)) {
        uprobe_release(uprobe_pic);
        uprobe_release(uprobe_subpic);
        return NULL;
    }

    struct upipe *upipe = upipe_bmd_sink_to_upipe(upipe_bmd_sink);
    upipe_init(upipe, mgr, uprobe);

    upipe_bmd_sink_init_sub_inputs(upipe);
    upipe_bmd_sink_init_sub_mgr(upipe);
    upipe_bmd_sink_init_urefcount(upipe);

    /* Initalise subpipes */
    upipe_bmd_sink_sub_init(upipe_bmd_sink_sub_to_upipe(upipe_bmd_sink_to_pic_subpipe(upipe_bmd_sink)),
                            &upipe_bmd_sink->sub_mgr, uprobe_pic, true);
    upipe_bmd_sink_sub_init(upipe_bmd_sink_sub_to_upipe(upipe_bmd_sink_to_subpic_subpipe(upipe_bmd_sink)),
                            &upipe_bmd_sink->sub_mgr, uprobe_subpic, true);

    const unsigned max_samples = (uint64_t)48000 * 1001 / 24000;
    const size_t audio_buf_size = max_samples * DECKLINK_CHANNELS * sizeof(int32_t);
    upipe_bmd_sink->pic_subpipe.audio_buf = (int32_t*)malloc(audio_buf_size);

    upipe_bmd_sink->pts_offset = 0;
    upipe_bmd_sink->offset = 0;
    upipe_bmd_sink->uclock.refcount = upipe->refcount;
    upipe_bmd_sink->uclock.uclock_now = uclock_bmd_sink_now;
    upipe_bmd_sink->preroll = PREROLL_FRAMES;

    upipe_throw_ready(upipe);
    return upipe;
}

static int upipe_bmd_open_vid(struct upipe *upipe)
{
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_upipe(upipe);
    IDeckLinkOutput *deckLinkOutput = upipe_bmd_sink->deckLinkOutput;
    IDeckLinkDisplayModeIterator *displayModeIterator = NULL;
    char* displayModeName = NULL;
    IDeckLinkDisplayMode* displayMode = NULL;
    int err = UBASE_ERR_NONE;
    HRESULT result = E_NOINTERFACE;

    if (upipe_bmd_sink->displayMode) {
        upipe_bmd_sink_sub_flush_input(&upipe_bmd_sink->pic_subpipe.upipe);
        upipe_bmd_sink_sub_flush_input(&upipe_bmd_sink->subpic_subpipe.upipe);
        deckLinkOutput->StopScheduledPlayback(0, NULL, 0);
        deckLinkOutput->DisableAudioOutput();
        upipe_bmd_sink->displayMode->Release();
        upipe_bmd_sink->started = 0;
        upipe_bmd_sink->start_pts = 0;
        upipe_bmd_sink->preroll = PREROLL_FRAMES;
    }

    result = deckLinkOutput->GetDisplayModeIterator(&displayModeIterator);
    if (result != S_OK){
        upipe_err_va(upipe, "decklink card has no display modes");
        err = UBASE_ERR_EXTERNAL;
        goto end;
    }

    while ((result = displayModeIterator->Next(&displayMode)) == S_OK)
    {
        if (displayMode->GetDisplayMode() == upipe_bmd_sink->mode)
            break;

        displayMode->Release();
    }

    if (result != S_OK || displayMode == NULL)
    {
        uint32_t mode = htonl(upipe_bmd_sink->mode);
        fprintf(stderr, "Unable to get display mode %4.4s\n", (char*)&mode);
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

    /* disable video the shortest time possible, to keep clock running */

    upipe_bmd_sink->offset = uclock_now(&upipe_bmd_sink->uclock);
    deckLinkOutput->DisableVideoOutput();
    upipe_bmd_sink->pts_offset = 0;
    result = deckLinkOutput->EnableVideoOutput(displayMode->GetDisplayMode(),
                                               bmdVideoOutputVANC);
    if (result != S_OK)
    {
        fprintf(stderr, "Failed to enable video output. Is another application using the card?\n");
        err = UBASE_ERR_EXTERNAL;
        goto end;
    }

    result = deckLinkOutput->EnableAudioOutput(48000, bmdAudioSampleType32bitInteger,
            DECKLINK_CHANNELS, bmdAudioOutputStreamTimestamped);
    if (result != S_OK)
    {
        fprintf(stderr, "Failed to enable audio output. Is another application using the card?\n");
        err = UBASE_ERR_EXTERNAL;
        goto end;
    }

    upipe_bmd_sink->genlock_status = -1;

    if (upipe_bmd_sink->mode == bmdModePAL) {
        upipe_bmd_sink->sp.scanning         = 625; /* PAL */
        upipe_bmd_sink->sp.sampling_format  = VBI_PIXFMT_YUV420;
        upipe_bmd_sink->sp.sampling_rate    = 13.5e6;
        upipe_bmd_sink->sp.bytes_per_line   = 720;
        upipe_bmd_sink->sp.start[0]     = 6;
        upipe_bmd_sink->sp.count[0]     = 17;
        upipe_bmd_sink->sp.start[1]     = 319;
        upipe_bmd_sink->sp.count[1]     = 17;
        upipe_bmd_sink->sp.interlaced   = FALSE;
        upipe_bmd_sink->sp.synchronous  = FALSE;
        upipe_bmd_sink->sp.offset       = 128;
    } else if (upipe_bmd_sink->mode == bmdModeNTSC) {
        upipe_bmd_sink->sp.scanning         = 525; /* NTSC */
        upipe_bmd_sink->sp.sampling_format  = VBI_PIXFMT_YUV420;
        upipe_bmd_sink->sp.sampling_rate    = 13.5e6;
        upipe_bmd_sink->sp.bytes_per_line   = 720;
        upipe_bmd_sink->sp.interlaced   = FALSE;
        upipe_bmd_sink->sp.synchronous  = TRUE;
    }

end:
    if (displayModeIterator != NULL)
        displayModeIterator->Release();

    return err;
}

/** @internal @This asks to open the given device.
 *
 * @param upipe description structure of the pipe
 * @param uri URI
 * @return an error code
 */
static int upipe_bmd_sink_open_card(struct upipe *upipe)
{
    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_upipe(upipe);

    int err = UBASE_ERR_NONE;
    HRESULT result = E_NOINTERFACE;

    assert(!upipe_bmd_sink->deckLink);

    /* decklink interface interator */
    IDeckLinkIterator *deckLinkIterator = CreateDeckLinkIteratorInstance();
    if (!deckLinkIterator) {
        upipe_err_va(upipe, "decklink drivers not found");
        return UBASE_ERR_EXTERNAL;
    }

    /* get decklink interface handler */
    IDeckLink *deckLink = NULL;
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
        if (deckLink)
            deckLink->Release();
        goto end;
    }

    if (deckLink->QueryInterface(IID_IDeckLinkOutput,
                                 (void**)&upipe_bmd_sink->deckLinkOutput) != S_OK) {
        upipe_err_va(upipe, "decklink card has no output");
        err = UBASE_ERR_EXTERNAL;
        deckLink->Release();
        goto end;
    }

    result = upipe_bmd_sink->deckLinkOutput->SetScheduledFrameCompletionCallback(new callback(
                &upipe_bmd_sink->upipe, upipe_bmd_sink->deckLinkOutput, &upipe_bmd_sink->uclock));
    if (result != S_OK) {
        abort();
    }

    upipe_bmd_sink->deckLink = deckLink;

end:

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
        union {
            BMDDisplayMode mode_id;
            char mode_s[4];
        } u;
        strncpy(u.mode_s, v, sizeof(u.mode_s));
        upipe_bmd_sink->mode = htonl(u.mode_id);
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

    decklink_configuration->WriteConfigurationToPreferences();
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
    struct upipe_bmd_sink *bmd_sink = upipe_bmd_sink_from_upipe(upipe);

    switch (command) {
        case UPIPE_SET_URI:
            if (!bmd_sink->deckLink) {
                UBASE_RETURN(upipe_bmd_sink_open_card(upipe));
            }
            return upipe_bmd_open_vid(upipe);

        case UPIPE_GET_SUB_MGR: {
            struct upipe_mgr **p = va_arg(args, struct upipe_mgr **);
            return upipe_bmd_sink_get_sub_mgr(upipe, p);
        }

        case UPIPE_BMD_SINK_GET_PIC_SUB: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_BMD_SINK_SIGNATURE)
            struct upipe **upipe_p = va_arg(args, struct upipe **);
            *upipe_p =  upipe_bmd_sink_sub_to_upipe(
                            upipe_bmd_sink_to_pic_subpipe(
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
            *pp_uclock = &bmd_sink->uclock;
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

    upipe_bmd_sink_sub_free(upipe_bmd_sink_sub_to_upipe(&upipe_bmd_sink->pic_subpipe));
    upipe_bmd_sink_sub_free(upipe_bmd_sink_sub_to_upipe(&upipe_bmd_sink->subpic_subpipe));
    upipe_throw_dead(upipe);

    free(upipe_bmd_sink->pic_subpipe.audio_buf);

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
