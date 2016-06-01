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
#include <libzvbi.h>

#include "include/DeckLinkAPI.h"

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
    BMDDisplayMode mode;

    /** started flag **/
    int started;

    /** vanc/vbi temporary buffer **/
    uint16_t vanc_tmp[2][VANC_WIDTH*2];
    uint16_t *dc[2];

    /** closed captioning **/
    uint16_t cdp_hdr_sequence_cntr;

    /** OP47 teletext sequence counter **/
    uint16_t op47_sequence_counter[2];

    /** OP47 total number of teletext packets written **/
    int op47_number_of_packets[2];

    /** subpic queue **/
    struct uchain subpic_queue;

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

    /** last hardware clock read */
    uint64_t last_cr;

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

UBASE_FROM_TO(upipe_bmd_sink, upipe_mgr, sub_mgr, sub_mgr)
UBASE_FROM_TO(upipe_bmd_sink, upipe_bmd_sink_sub, pic_subpipe, pic_subpipe)
UBASE_FROM_TO(upipe_bmd_sink, upipe_bmd_sink_sub, sound_subpipe, sound_subpipe)
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
        struct upipe_mgr *sub_mgr, struct uprobe *uprobe)
{
    upipe_init(upipe, sub_mgr, uprobe);
    upipe_mgr_release(sub_mgr); /* do not reference super pipe */

    struct upipe_bmd_sink *upipe_bmd_sink = upipe_bmd_sink_from_sub_mgr(sub_mgr);
    struct upipe_bmd_sink_sub *upipe_bmd_sink_sub = upipe_bmd_sink_sub_from_upipe(upipe);
    upipe_bmd_sink_sub->upipe_bmd_sink = upipe_bmd_sink_to_upipe(upipe_bmd_sink);

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
    upipe_clean(upipe);
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

    if (upipe_bmd_sink_sub == &upipe_bmd_sink->subpic_subpipe) {
        ulist_add(&upipe_bmd_sink->subpic_queue, uref_to_uchain(uref));
        return true;
    }

    uint64_t pts = 0;
    if (likely(ubase_check(uref_clock_get_pts_sys(uref, &pts)))) {
        uint64_t now = uclock_now(&upipe_bmd_sink->uclock);
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
        int sd = upipe_bmd_sink->mode == bmdModePAL || upipe_bmd_sink->mode == bmdModeNTSC;
        int ttx = upipe_bmd_sink->mode == bmdModePAL || upipe_bmd_sink->mode == bmdModeHD1080i50;
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
            upipe_bmd_sink_clear_vanc(upipe_bmd_sink->vanc_tmp[0]);
            upipe_bmd_sink_start_anc(upipe, upipe_bmd_sink->vanc_tmp[0], 0, 0x61, 0x1);
            upipe_bmd_sink_write_cdp(upipe, pic_data, pic_data_size, &upipe_bmd_sink->vanc_tmp[0][ANC_START_LEN]);
            upipe_bmd_sink_calc_parity_checksum(upipe, 0);

            upipe_bmd_sink_encode_v210(upipe, (uint32_t*)vanc, 0, sd);
        }

        /* Loop through subpic data */
        //printf("\n subpic depth %i \n", ulist_depth(&upipe_bmd_sink->subpic_queue));
        struct uchain *uchain, *uchain_tmp;
        ulist_delete_foreach(&upipe_bmd_sink->subpic_queue, uchain, uchain_tmp) {
            //printf("\n VIDEO PTS %"PRIu64" \n", pts );
            struct uref *uref = uref_from_uchain(uchain);
            uint64_t subpic_pts = 0;
            uref_clock_get_pts_sys(uref, &subpic_pts);
            subpic_pts += upipe_bmd_sink->subpic_subpipe.latency;
            //printf("\n SUBPIC PTS %"PRIu64" \n", subpic_pts );

            /* Delete old urefs */
            if (subpic_pts + (UCLOCK_FREQ/25) < pts) {
                ulist_delete(uchain);
                uref_free(uref);
                continue;
            }

            /* Choose the closest subpic in the past */
            if (ttx && pts - (UCLOCK_FREQ/25) < subpic_pts) {
                //printf("\n CHOSEN SUBPIC %"PRIu64" \n", subpic_pts);
                const uint8_t *buf;
                int size = -1;
                uref_block_read(uref, 0, &size, &buf);
                upipe_bmd_sink_extract_ttx(upipe, ancillary, buf, size, sd);
                uref_block_unmap(uref, 0);
                ulist_delete(uchain);
                uref_free(uref);
                break;
            }
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
    else {
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

static BMDDisplayMode upipe_bmd_mode_from_flow_def(struct upipe_bmd_sink *upipe_bmd_sink, struct uref *flow_def)
{
    struct upipe *upipe = &upipe_bmd_sink->upipe;
    IDeckLinkOutput *deckLinkOutput = upipe_bmd_sink->deckLinkOutput;
    char *displayModeName = NULL;
    BMDDisplayMode bmdMode = bmdModeUnknown;

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

static int upipe_bmd_open_vid(struct upipe *upipe);

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

        BMDDisplayMode bmdMode = upipe_bmd_mode_from_flow_def(upipe_bmd_sink, flow_def);
        if (bmdMode != upipe_bmd_sink->mode) {
            uint32_t prev_mode = htonl(upipe_bmd_sink->mode);
            uint32_t new_mode = htonl(bmdMode);
            upipe_notice_va(upipe, "Changing mode from %4.4s to %4.4s",
                    &prev_mode, &new_mode);
            upipe_bmd_sink->mode = bmdMode;
            if (!ubase_check(upipe_bmd_open_vid(&upipe_bmd_sink->upipe))) {
                upipe_err_va(upipe, "Could not change mode");
                return UBASE_ERR_EXTERNAL;
            }
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

    if (upipe_bmd_sink->deckLinkOutput) {
        HRESULT res = upipe_bmd_sink->deckLinkOutput->GetHardwareReferenceClock(
                UCLOCK_FREQ, &hardware_time, &time_in_frame, &ticks_per_frame);
        if (res != S_OK) {
            upipe_err_va(upipe, "\t\tCouldn't read hardware clock: 0x%08lx", res);
            hardware_time = upipe_bmd_sink->last_cr;
        } else
            upipe_bmd_sink->last_cr = hardware_time;
    } else
        upipe_err_va(upipe, "No output configured");

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

    ulist_init(&upipe_bmd_sink->subpic_queue);

    upipe_bmd_sink->last_cr = 0;
    upipe_bmd_sink->uclock.uclock_now = uclock_bmd_sink_now;

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
        //uclock_set_offset(&upipe_bmd_sink->uclock.uclock); SHIT
        upipe_bmd_sink_sub_flush_input(&upipe_bmd_sink->pic_subpipe.upipe);
        upipe_bmd_sink_sub_flush_input(&upipe_bmd_sink->sound_subpipe.upipe);
        upipe_bmd_sink_sub_flush_input(&upipe_bmd_sink->subpic_subpipe.upipe);
        deckLinkOutput->StopScheduledPlayback(0, NULL, 0);
        deckLinkOutput->DisableAudioOutput();
        upipe_bmd_sink->displayMode->Release();
        upipe_bmd_sink->started = 0;
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
    deckLinkOutput->DisableVideoOutput();
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

    struct uchain *uchain, *uchain_tmp;
    ulist_delete_foreach(&upipe_bmd_sink->subpic_queue, uchain, uchain_tmp) {
        struct uref *uref = uref_from_uchain(uchain);
        ulist_delete(uchain);
        uref_free(uref);
    }

    upipe_bmd_sink_sub_free(upipe_bmd_sink_sub_to_upipe(&upipe_bmd_sink->pic_subpipe));
    upipe_bmd_sink_sub_free(upipe_bmd_sink_sub_to_upipe(&upipe_bmd_sink->sound_subpipe));
    upipe_bmd_sink_sub_free(upipe_bmd_sink_sub_to_upipe(&upipe_bmd_sink->subpic_subpipe));
    upipe_throw_dead(upipe);

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
