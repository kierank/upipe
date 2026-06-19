/*
 * Copyright (C) 2026 Open Broadcast Systems Ltd
 *
 * Authors: Kieran Kunhya
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 */

/** @file
 * @short Upipe libzvbi CEA-608 closed caption decoding module
 *
 * This module reads the CEA-708 cc_data triplets carried as the @ref
 * uref_pic_get_cea_708 opaque attribute on incoming picture urefs (the same
 * attribute consumed by the zvbi encoder), feeds the CEA-608 (line 21)
 * captions they contain into a libzvbi decoder and renders the selected
 * caption channel to an alpha-keyed (RGBA) sub-picture that can be overlaid
 * on the video (e.g. with @ref upipe_blit or @ref upipe_subpic_schedule).
 *
 * Note: libzvbi only renders CEA-608 captions (cc_type 0 and 1, fields 1 and
 * 2). CEA-708 DTVCC service blocks (cc_type 2 and 3) are not decoded.
 *
 * The input is not forwarded: this pipe consumes picture urefs carrying the
 * caption attribute and only emits sub-picture bitmaps. The video itself is
 * expected to be routed to the overlay separately.
 */

#ifndef _UPIPE_ZVBI_UPIPE_ZVBIDEC_H_
/** @hidden */
#define _UPIPE_ZVBI_UPIPE_ZVBIDEC_H_
#ifdef __cplusplus
extern "C" {
#endif

#include "upipe/upipe.h"

#define UPIPE_ZVBIDEC_SIGNATURE UBASE_FOURCC('z','v','b','d')

/** @This extends upipe_command with specific commands for zvbidec pipes. */
enum upipe_zvbidec_command {
    UPIPE_ZVBIDEC_SENTINEL = UPIPE_CONTROL_LOCAL,

    /** sets the caption channel to render (int) */
    UPIPE_ZVBIDEC_SET_CHANNEL,
    /** gets the caption channel being rendered (int *) */
    UPIPE_ZVBIDEC_GET_CHANNEL,
};

/** @This identifies the CEA-608 caption channels. The value is the index
 * passed to @ref upipe_zvbidec_set_channel; it maps to a libzvbi caption
 * page number (channel + 1). */
enum upipe_zvbidec_channel {
    /** captions, channel 1 (default) */
    UPIPE_ZVBIDEC_CC1 = 0,
    /** captions, channel 2 */
    UPIPE_ZVBIDEC_CC2,
    /** captions, channel 3 */
    UPIPE_ZVBIDEC_CC3,
    /** captions, channel 4 */
    UPIPE_ZVBIDEC_CC4,
    /** text, channel 1 */
    UPIPE_ZVBIDEC_TEXT1,
    /** text, channel 2 */
    UPIPE_ZVBIDEC_TEXT2,
    /** text, channel 3 */
    UPIPE_ZVBIDEC_TEXT3,
    /** text, channel 4 */
    UPIPE_ZVBIDEC_TEXT4,
};

/** @This sets the caption channel to render.
 *
 * @param upipe description structure of the pipe
 * @param channel caption channel (@see enum upipe_zvbidec_channel)
 * @return an error code
 */
static inline int upipe_zvbidec_set_channel(struct upipe *upipe, int channel)
{
    return upipe_control(upipe, UPIPE_ZVBIDEC_SET_CHANNEL,
                         UPIPE_ZVBIDEC_SIGNATURE, channel);
}

/** @This gets the caption channel being rendered.
 *
 * @param upipe description structure of the pipe
 * @param channel_p filled in with the caption channel
 * @return an error code
 */
static inline int upipe_zvbidec_get_channel(struct upipe *upipe, int *channel_p)
{
    return upipe_control(upipe, UPIPE_ZVBIDEC_GET_CHANNEL,
                         UPIPE_ZVBIDEC_SIGNATURE, channel_p);
}

/** @This returns the management structure for zvbidec pipes.
 *
 * @return pointer to manager
 */
struct upipe_mgr *upipe_zvbidec_mgr_alloc(void);

#ifdef __cplusplus
}
#endif
#endif
