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
#include <upipe/uclock_std.h>
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

    IDeckLinkDisplayMode* displayMode;

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
#endif

    if (v != NULL)
        return udict_set_string(upipe_bmd_sink->options->udict, v,
                                UDICT_TYPE_STRING, k);
    else
        udict_delete(upipe_bmd_sink->options->udict, UDICT_TYPE_STRING, k);
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

    upipe_release(upipe_bmd_sink_sub_to_upipe(&upipe_bmd_sink->pic_subpipe));
    upipe_release(upipe_bmd_sink_sub_to_upipe(&upipe_bmd_sink->sound_subpipe));
    upipe_release(upipe_bmd_sink_sub_to_upipe(&upipe_bmd_sink->subpic_subpipe));
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
