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
 * @short Upipe blackmagic sink
 */

#ifndef _UPIPE_BMD_SINK_H_
/** @hidden */
#define _UPIPE_BMD_SINK_H_
#ifdef __cplusplus
extern "C" {
#endif

#include <upipe/upipe.h>

#define UPIPE_BMD_SINK_SIGNATURE UBASE_FOURCC('b','m','d','k')
#define UPIPE_BMD_SINK_INPUT_SIGNATURE UBASE_FOURCC('b','m','d','i')

/** @This extends upipe_command with specific commands for avformat source. */
enum upipe_bmd_sink_command {
    UPIPE_BMD_SINK_SENTINEL = UPIPE_CONTROL_LOCAL,

    /** returns the pic subpipe (struct upipe **) */
    UPIPE_BMD_SINK_GET_PIC_SUB,
    /** returns the sound subpipe (struct upipe **) */
    UPIPE_BMD_SINK_GET_SOUND_SUB,
    /** returns the subpic subpipe (struct upipe **) */
    UPIPE_BMD_SINK_GET_SUBPIC_SUB,

    /** returns the uclock (struct uclock *) **/
    UPIPE_BMD_SINK_GET_UCLOCK
};

/** @This returns the management structure for all bmd sinks.
 *
 * @return pointer to manager
 */
struct upipe_mgr *upipe_bmd_sink_mgr_alloc(void);

/** @This returns the pic subpipe. The refcount is not incremented so you
 * have to use it if you want to keep the pointer.
 *
 * @param upipe description structure of the super pipe
 * @param upipe_p filled in with a pointer to the pic subpipe
 * @return an error code
 */
static inline int upipe_bmd_sink_get_pic_sub(struct upipe *upipe,
                                               struct upipe **upipe_p)
{
    return upipe_control(upipe, UPIPE_BMD_SINK_GET_PIC_SUB,
                          UPIPE_BMD_SINK_SIGNATURE, upipe_p);
}

/** @This returns the sound subpipe. The refcount is not incremented so you
 * have to use it if you want to keep the pointer.
 *
 * @param upipe description structure of the super pipe
 * @param upipe_p filled in with a pointer to the sound subpipe
 * @return an error code
 */
static inline int upipe_bmd_sink_get_sound_sub(struct upipe *upipe,
                                                 struct upipe **upipe_p)
{
    return upipe_control(upipe, UPIPE_BMD_SINK_GET_SOUND_SUB,
                          UPIPE_BMD_SINK_SIGNATURE, upipe_p);
}

/** @This returns the subpic subpipe. The refcount is not incremented so you
 * have to use it if you want to keep the pointer.
 *
 * @param upipe description structure of the super pipe
 * @param upipe_p filled in with a pointer to the subpic subpipe
 * @return an error code
 */
static inline int upipe_bmd_sink_get_subpic_sub(struct upipe *upipe,
                                                  struct upipe **upipe_p)
{
    return upipe_control(upipe, UPIPE_BMD_SINK_GET_SUBPIC_SUB,
                          UPIPE_BMD_SINK_SIGNATURE, upipe_p);
}

/** @This returns the bmd_sink uclock. The refcount is not incremented so you
 * have to use it if you want to keep the pointer.
 *
 * @param upipe description structure of the super pipe
 * @param upipe_p filled in with a pointer to the subpic subpipe
 * @return an error code
 */
static inline int upipe_bmd_sink_get_uclock(struct upipe *upipe,
                                              struct uclock **uclock_p)
{
    return upipe_control(upipe, UPIPE_BMD_SINK_GET_UCLOCK,
                          UPIPE_BMD_SINK_SIGNATURE, uclock_p);
}

/** @This allocates and initializes a bmd sink pipe.
 *
 * @param mgr management structure for bmd sink type
 * @param uprobe structure used to raise events for the super pipe
 * @param uprobe_pic structure used to raise events for the pic subpipe
 * @param uprobe_sound structure used to raise events for the sound subpipe
 * @param uprobe_subpic structure used to raise events for the subpic subpipe
 * @return pointer to allocated pipe, or NULL in case of failure
 */
static inline struct upipe *upipe_bmd_sink_alloc(struct upipe_mgr *mgr,
                                                    struct uprobe *uprobe,
                                                    struct uprobe *uprobe_pic,
                                                    struct uprobe *uprobe_sound,
                                                    struct uprobe *uprobe_subpic)
{
    return upipe_alloc(mgr, uprobe, UPIPE_BMD_SINK_SIGNATURE,
                        uprobe_pic, uprobe_sound, uprobe_subpic);
}


#ifdef __cplusplus
}
#endif
#endif