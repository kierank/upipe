/*
 * Copyright (C) 2012 OpenHeadend S.A.R.L.
 *
 * Authors: Christophe Massiot
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston MA 02110-1301, USA.
 */

/** @file
 * @short Upipe module decapsulating (removing TS header) TS packets
 */

#ifndef _UPIPE_TS_UPIPE_TS_DECAPS_H_
/** @hidden */
#define _UPIPE_TS_UPIPE_TS_DECAPS_H_

#include <upipe/upipe.h>
#include <upipe-ts/upipe_ts_demux.h>

#define UPIPE_TS_DECAPS_SIGNATURE UBASE_FOURCC('t','s','d','c')

/** @This extends uprobe_event with specific events for ts decaps. */
enum uprobe_ts_decaps_event {
    UPROBE_TS_DECAPS_SENTINEL = UPROBE_TS_DEMUX_DECAPS,

    /** a PCR was found in the given uref (struct uref *, uint64_t) */
    UPROBE_TS_DECAPS_PCR,
};

/** @This returns the management structure for all ts_decaps pipes.
 *
 * @return pointer to manager
 */
struct upipe_mgr *upipe_ts_decaps_mgr_alloc(void);

#endif
