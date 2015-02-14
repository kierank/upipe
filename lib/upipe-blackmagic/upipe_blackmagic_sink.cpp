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
#include <upipe/upipe.h>
#include <upipe/upipe_helper_upipe.h>
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

#include "include/DeckLinkAPI.h"

/** @internal @This is the private context of an output of an bmd_sink sink
 * pipe. */
struct upipe_bmd_sink_sub {
    struct upipe *upipe_bmd_sink;

    /** refcount management structure */
    struct urefcount urefcount;

    /** public upipe structure */
    struct upipe upipe;
};

/** super-set of the uclock structure with additional local members */
struct uclock_bmd_sink {
    /** refcount management structure */
    struct urefcount urefcount;

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

    struct uref *options;

    int started;

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

UBASE_FROM_TO(upipe_bmd_sink, upipe_mgr, sub_mgr, sub_mgr)
UBASE_FROM_TO(upipe_bmd_sink, upipe_bmd_sink_sub, pic_subpipe, pic_subpipe)
UBASE_FROM_TO(upipe_bmd_sink, upipe_bmd_sink_sub, sound_subpipe, sound_subpipe)
UBASE_FROM_TO(upipe_bmd_sink, upipe_bmd_sink_sub, subpic_subpipe, subpic_subpipe)

UBASE_FROM_TO(upipe_bmd_sink, uclock_bmd_sink, uclock_bmd_sink, uclock)


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

    upipe_throw_ready(upipe);
}

static void upipe_bmd_sink_sub_free(struct upipe *upipe)
{
    struct upipe_bmd_sink_sub *upipe_bmd_sink_sub = upipe_bmd_sink_sub_from_upipe(upipe);
    upipe_throw_dead(upipe);
    upipe_bmd_sink_sub_clean_urefcount(upipe);
    uprobe_release(upipe->uprobe);
    upipe_release(upipe_bmd_sink_sub->upipe_bmd_sink);
}

/** @internal @This handles input uref.
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
    struct upipe_bmd_sink_sub *upipe_bmd_sink_sub =
        upipe_bmd_sink_sub_from_upipe(upipe);

    if (!upipe_bmd_sink->deckLink) {
        upipe_err_va(upipe, "DeckLink card not ready");
        return;
    }

    HRESULT result;
    const char *v210 = "u10y10v10y10u10y10v10y10u10y10v10y10";

    if (upipe_bmd_sink_sub == &upipe_bmd_sink->pic_subpipe){
        int w = upipe_bmd_sink->displayMode->GetWidth();
        int h = upipe_bmd_sink->displayMode->GetHeight();

        if (!upipe_bmd_sink->started){
            upipe_bmd_sink->deckLinkOutput->StartScheduledPlayback(uref->date_sys, UCLOCK_FREQ, 1.0);
            upipe_bmd_sink->started = 1;
        }

        int dst_stride = (((w + 47) / 48) * 48) * 8 / 3;
        IDeckLinkMutableVideoFrame *video_frame;
        result = upipe_bmd_sink->deckLinkOutput->CreateVideoFrame(w, h, dst_stride, bmdFormat10BitYUV, bmdFrameFlagDefault, &video_frame);
        if (result != S_OK) {
            upipe_err_va(upipe, "Could not create frame");
            uref_free(uref);
            return;
        }

        void *frame_bytes;
        video_frame->GetBytes((void**)&frame_bytes);


        size_t stride;
        const uint8_t *plane;
        if (unlikely(!ubase_check(uref_pic_plane_size(uref, v210, &stride,
                                          NULL, NULL, NULL)) ||
                     !ubase_check(uref_pic_plane_read(uref, v210, 0, 0, -1, -1,
                                          &plane)))) {
            upipe_err_va(upipe, "Could not read v210 plane");
            uref_free(uref);
            return;
        }

        memcpy(frame_bytes, plane, stride * h);
        uref_pic_plane_unmap(uref, v210, 0, 0, -1, -1);

        BMDTimeValue timeValue;
        BMDTimeScale timeScale;
        upipe_bmd_sink->displayMode->GetFrameRate(&timeValue, &timeScale);

        result = upipe_bmd_sink->deckLinkOutput->ScheduleVideoFrame(video_frame, uref->date_sys, UCLOCK_FREQ * timeValue / timeScale, UCLOCK_FREQ);
        if( result != S_OK )
            upipe_err_va(upipe, "DROPPED FRAME");

        video_frame->Release();
    }
    else if(upipe_bmd_sink_sub == &upipe_bmd_sink->sound_subpipe && upipe_bmd_sink->started){
        size_t size = 0;
        uref_sound_size(uref, &size, NULL);

        const int32_t *buffers[1];
        uref_sound_read_int32_t(uref, 0, size, buffers, 1);

        uint32_t written;
        result = upipe_bmd_sink->deckLinkOutput->ScheduleAudioSamples((void*)buffers[0], size, uref->date_sys, UCLOCK_FREQ, &written);
        uref_sound_unmap(uref, 0, size, 1);

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
    }


    uref_free(uref);
    return;
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
    if (flow_def == NULL)
        return UBASE_ERR_INVALID;

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
    
    upipe_bmd_sink->deckLinkOutput->GetHardwareReferenceClock(UCLOCK_FREQ, &hardware_time,
                                                              &time_in_frame, &ticks_per_frame);

    return (uint64_t)hardware_time;
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
    IDeckLinkDisplayModeIterator *displayModeIterator = NULL;
    char* displayModeName = NULL;
    IDeckLinkDisplayMode* displayMode = NULL;
    int err = UBASE_ERR_NONE;
    int card_idx = 0;
    HRESULT result = E_NOINTERFACE;
    struct udict *dict = NULL;
    const char *opt;

    // FIXME check things here

    if (upipe_bmd_sink->options)
        dict = upipe_bmd_sink->options->udict;

    /* decklink interface interator */
    deckLinkIterator = CreateDeckLinkIteratorInstance();
    if (!deckLinkIterator) {
        upipe_err_va(upipe, "decklink drivers not found");
        err = UBASE_ERR_EXTERNAL;
        goto end;
    }

    if (dict && !udict_get_string(dict, &opt, UDICT_TYPE_STRING, "card-index"))
        card_idx = atoi(opt);

    /* get decklink interface handler */
    for (int i = 0; i <= card_idx; i++) {
        if (deckLink)
            deckLink->Release();
        result = deckLinkIterator->Next(&deckLink);
        if (result != S_OK)
            break;
    }

    if (result != S_OK) {
        upipe_err_va(upipe, "decklink card %d not found", card_idx);
        //err = UBASE_ERR_EXTERNAL;
        goto end;
    }

    /* get decklink output handler */
    IDeckLinkOutput *deckLinkOutput;
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

    if (!dict || udict_get_string(dict, &opt, UDICT_TYPE_STRING, "mode"))
        opt = NULL;

    if (!opt || strlen(opt) != 4)
        opt = "pal ";

    while ((result = displayModeIterator->Next(&displayMode)) == S_OK)
    {
        union {
            BMDDisplayMode mode_id;
            char mode_s[4];
        } u;
        u.mode_id = ntohl(displayMode->GetDisplayMode());

        if (!strncmp(u.mode_s, opt, 4))
            break;

        displayMode->Release();
    }

    if (result != S_OK || displayMode == NULL)
    {
        fprintf(stderr, "Unable to get display mode %s\n", opt);
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

        ;
    result = deckLinkOutput->EnableVideoOutput(displayMode->GetDisplayMode(),
                                               bmdVideoOutputFlagDefault);
    if (result != S_OK)
    {
        fprintf(stderr, "Failed to enable video output. Is another application using the card?\n");
        err = UBASE_ERR_EXTERNAL;
        goto end;
    }

    result = deckLinkOutput->EnableAudioOutput(48000, bmdAudioSampleType32bitInteger, 2, bmdAudioOutputStreamTimestamped);
    if (result != S_OK)
    {
        fprintf(stderr, "Failed to enable audio output. Is another application using the card?\n");
        err = UBASE_ERR_EXTERNAL;
        goto end;
    }

    upipe_bmd_sink->deckLinkOutput = deckLinkOutput;
    upipe_bmd_sink->deckLink = deckLink;

    uclock_bmd_sink_init(&upipe_bmd_sink->uclock);
    urefcount_use(uclock_bmd_sink_to_urefcount(&upipe_bmd_sink->uclock));

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

// FIXME
#if 0 
    if (upipe_bmd_sink->options == NULL) {
        struct uref_mgr *uref_mgr;
        UBASE_RETURN(upipe_throw_need_uref_mgr(upipe, &uref_mgr))
        upipe_bmd_sink->options = uref_alloc_control(uref_mgr);
        uref_mgr_release(uref_mgr);
    }

    if (v != NULL)
        return udict_set_string(upipe_bmd_sink->options->udict, v,
                                UDICT_TYPE_STRING, k);
    else
        udict_delete(upipe_bmd_sink->options->udict, UDICT_TYPE_STRING, k);
#endif

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

    uref_free(upipe_bmd_sink->options);
    upipe_bmd_sink_clean_urefcount(upipe);
    upipe_bmd_sink_free_void(upipe);
}

/** upipe_bmd_sink (/dev/bmd_sink) */
static struct upipe_mgr upipe_bmd_sink_mgr = {
    /* .refcount = */ NULL,
    /* .signature = */ UPIPE_BMD_SINK_SIGNATURE,

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
