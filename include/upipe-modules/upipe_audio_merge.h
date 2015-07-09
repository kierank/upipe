/*
 * Copyright (C) 2015 Open Broadcast Systems Ltd.
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
 * @short Upipe module merging audio packets into a single buffer
 */

#ifndef _UPIPE_MODULES_UPIPE_AUDIO_MERGE_H_
/** @hidden */
#define _UPIPE_MODULES_UPIPE_AUDIO_MERGE_H_
#ifdef __cplusplus
extern "C" {
#endif

#include <upipe/upipe.h>
#include <upipe/uref_attr.h>

#define UPIPE_AUDIO_MERGE_SIGNATURE UBASE_FOURCC('a','m','g','l')
#define UPIPE_AUDIO_MERGE_INPUT_SIGNATURE UBASE_FOURCC('a','m','g','i')

UREF_ATTR_SMALL_UNSIGNED(audio_merge, channel_index, "audio_merge.channel_index", channel index to use)

/** @This returns the management structure for all audio_merge pipes.
 *
 * @return pointer to manager
 */
struct upipe_mgr *upipe_audio_merge_mgr_alloc(void);

#ifdef __cplusplus
}
#endif
#endif
