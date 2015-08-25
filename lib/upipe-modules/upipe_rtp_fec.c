/** @file
 * @short Upipe RTP FEC module
 */

#include <upipe/ubase.h>
#include <upipe/uprobe.h>
#include <upipe/uref.h>
#include <upipe/uref_block.h>
#include <upipe/uref_block_flow.h>
#include <upipe/ubuf.h>
#include <upipe/uref_pic_flow.h>
#include <upipe/uref_pic.h>
#include <upipe/upipe.h>
#include <upipe/ulist.h>
#include <upipe/uref_flow.h>
#include <upipe/uref_dump.h>
#include <upipe/upump.h>
#include <upipe/upipe_helper_upipe.h>
#include <upipe/upipe_helper_urefcount.h>
#include <upipe/upipe_helper_flow.h>
#include <upipe/upipe_helper_flow_def.h>
#include <upipe/upipe_helper_output.h>
#include <upipe/upipe_helper_void.h>
#include <upipe/upipe_helper_uref_mgr.h>
#include <upipe/upipe_helper_upump_mgr.h>
#include <upipe/upipe_helper_upump.h>

#include <upipe-modules/upipe_rtp_decaps.h>
#include <upipe-modules/upipe_rtp_fec.h>
#include <bitstream/ietf/rtp.h>
#include <bitstream/smpte/2022_1_fec.h>

#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>

/** @hidden */
static int upipe_rtp_fec_check(struct upipe *upipe, struct uref *flow_format);

/** @internal @This is the private context of an output of an upipe_rtp_fec sub
 * pipe. */
struct upipe_rtp_fec_sub {
    /** refcount management structure */
    struct urefcount urefcount;
    /** structure for double-linked lists */
    struct uchain uchain;

    /** buffered urefs */
    struct uchain urefs;

    /** public upipe structure */
    struct upipe upipe;
};

/** upipe_rtp_fec structure with rtp-fec parameters */
struct upipe_rtp_fec {
    /** refcount management structure */
    struct urefcount urefcount;

    /** uref manager */
    struct uref_mgr *uref_mgr;
    /** uref manager request */
    struct urequest uref_mgr_request;

    /** source manager */
    struct upipe_mgr sub_mgr;

    int64_t pkts_since_last_fec;

    int cols;
    int rows;

    uint64_t first_seqnum;
    uint64_t last_seqnum;

    uint64_t last_matrix_snbase;
    uint64_t last_row_fec_snbase;

    /** main subpipe **/
    struct upipe_rtp_fec_sub main_subpipe;
    /** col subpipe */
    struct upipe_rtp_fec_sub col_subpipe;
    /** row subpipe */
    struct upipe_rtp_fec_sub row_subpipe;

    struct uchain main_queue;
    struct uchain row_queue;

    int pump_start;
    uint64_t first_exit_time;
    uint64_t first_timestamp;
    uint64_t last_timestamp;
    uint64_t last_timestamp_raw;

    /** output pipe */
    struct upipe *output;
    /** flow_definition packet */
    struct uref *flow_def;
    /** output state */
    enum upipe_helper_output_state output_state;
    /** list of output requests */
    struct uchain request_list;

    /** public upipe structure */
    struct upipe upipe;
};

UPIPE_HELPER_UPIPE(upipe_rtp_fec, upipe, UPIPE_RTP_FEC_SIGNATURE);
UPIPE_HELPER_UREFCOUNT(upipe_rtp_fec, urefcount, upipe_rtp_fec_free);
UPIPE_HELPER_VOID(upipe_rtp_fec);

UPIPE_HELPER_OUTPUT(upipe_rtp_fec, output, flow_def, output_state, request_list)

UPIPE_HELPER_UREF_MGR(upipe_rtp_fec, uref_mgr, uref_mgr_request,
                      upipe_rtp_fec_check,
                      upipe_rtp_fec_register_output_request,
                      upipe_rtp_fec_unregister_output_request)

UPIPE_HELPER_UPIPE(upipe_rtp_fec_sub, upipe, UPIPE_RTP_FEC_INPUT_SIGNATURE)

UBASE_FROM_TO(upipe_rtp_fec, upipe_mgr, sub_mgr, sub_mgr)
UBASE_FROM_TO(upipe_rtp_fec, upipe_rtp_fec_sub, main_subpipe, main_subpipe)
UBASE_FROM_TO(upipe_rtp_fec, upipe_rtp_fec_sub, col_subpipe, col_subpipe)
UBASE_FROM_TO(upipe_rtp_fec, upipe_rtp_fec_sub, row_subpipe, row_subpipe)

/** @internal @This initializes an subpipe of a bmd sink pipe.
 *
 * @param upipe pointer to subpipe
 * @param sub_mgr manager of the subpipe
 * @param uprobe structure used to raise events by the subpipe
 */
static void upipe_rtp_fec_sub_init(struct upipe *upipe,
        struct upipe_mgr *sub_mgr, struct uprobe *uprobe)
{
    struct upipe_rtp_fec *upipe_rtp_fec = upipe_rtp_fec_from_sub_mgr(sub_mgr);
    upipe_init(upipe, sub_mgr, uprobe);
    upipe->refcount = &upipe_rtp_fec->urefcount;

    upipe_throw_ready(upipe);
}

static void upipe_rtp_fec_xor_c(uint8_t *dst, uint8_t *src, size_t len)
{
    for( int i = 0; i < len; i++ )
        dst[i] ^= src[i];
}

/* based off Live555 code */
static inline int seq_num_lt(uint16_t s1, uint16_t s2)
 {
    /* a 'less-than' on 16-bit sequence numbers */
    int diff = s2-s1;
    if (diff > 0)
        return (diff < 0x8000);
    else if (diff < 0)
        return (diff < -0x8000);
    else
        return 0;
}

static void clear_fec_list(struct uchain *fec_list, uint64_t last_fec_snbase)
{
    struct uchain *uchain, *uchain_tmp;
    uint8_t *fec_header;
    size_t size = FEC_HEADER_SIZE;

    // FIXME sort this
    /* make sure we don't have old row fec packets in the queue */
    ulist_delete_foreach (fec_list, uchain, uchain_tmp) {
        struct uref *fec_uref = uref_from_uchain(uchain);
        uref_block_read(fec_uref, 0, &size, &fec_header);
        uint64_t snbase_low = smpte_fec_get_snbase_low(fec_header);
        uref_block_unmap(fec_uref, 0);
        //printf("\n sqnlt %u %u %i \n", snbase_low, last_row_fec_snbase, seq_num_lt(snbase_low, last_row_fec_snbase) );

        if( seq_num_lt(snbase_low, last_fec_snbase) )
        {
            ulist_delete(uchain);
            uref_free(fec_uref);
            printf("\n deleting snbase_low %"PRIu64" last_fec_snbase %"PRIu64" \n", snbase_low, last_fec_snbase );
        }

        //printf("\n lastrow %"PRIu64" snbaselow %"PRIu64" \n", last_row_fec_snbase, snbase_low );
    }
}

/* MATCH UPIPE REORDER */
static void insert_corrected_uref(struct upipe_rtp_fec *upipe_rtp_fec, struct uref *uref)
{
    int dup = 0, ooo = 0;
    struct uchain *uchain, *uchain_tmp;
    uint64_t new_seqnum = 0;
    uref_rtp_get_seqnum(uref, &new_seqnum);

    printf("\n INSERT \n");

    ulist_delete_foreach_reverse(&upipe_rtp_fec->main_queue, uchain, uchain_tmp) {
        struct uref *cur_uref = uref_from_uchain(uchain);
        uint64_t seqnum = 0;
        uref_rtp_get_seqnum(cur_uref, &seqnum);

        printf("\n checking seqnum %u new_seqnum %u \n", seqnum, new_seqnum);
        if( seq_num_lt(new_seqnum, seqnum) )
        {
            if (ulist_is_first(&upipe_rtp_fec->main_queue, uchain)){
                printf("\n FIRST insert %u before %u \n", new_seqnum, seqnum );
                uref_clock_delete_date_sys(uref);
                ulist_insert(uchain->prev, uchain, uref_to_uchain(uref));
                ooo = 1;
                break;
            }
            else {
                struct uref *prev_uref = uref_from_uchain(uchain->prev);
                uint64_t prev_seqnum = 0;
                uref_rtp_get_seqnum(prev_uref, &prev_seqnum);
                if( !seq_num_lt(new_seqnum, prev_seqnum) && !(new_seqnum == prev_seqnum) )
                {
                    printf("\n insert %u before %u \n", new_seqnum, seqnum );
                    uref_clock_delete_date_sys(uref);
                    ulist_insert(uchain->prev, uchain, uref_to_uchain(uref));
                    ooo = 1;
                    break;
                }
            }
        }
        else if( new_seqnum == seqnum ) /* Duplicate packet */
        {
            printf("\n duplicate \n");
            dup = 1;
            uref_free(uref);
            break;
        }
        else
            break;
    }


    /* Add to end if normal packet */
    if ((!dup && !ooo)) {
        ulist_add(&upipe_rtp_fec->main_queue, uref_to_uchain(uref));
        printf("\n adding %u \n", new_seqnum);
    }
}

static int upipe_rtp_fec_apply_row_fec(struct upipe_rtp_fec *upipe_rtp_fec,
                                        uint64_t last_row_fec_snbase)
{
    struct uchain *uchain, *uchain_tmp;
    uint64_t seqnum;
    uint8_t *fec_header;
    size_t size = FEC_HEADER_SIZE;

    uint16_t seqnum_list[50];
    bool found_seqnum[50] = {0};

    /* get rid of old FEC packets */
    clear_fec_list(&upipe_rtp_fec->row_queue, last_row_fec_snbase);

    struct uref *fec_uref = uref_from_uchain(ulist_pop(&upipe_rtp_fec->row_queue));
    if( !fec_uref )
        return 0;

    uref_block_read(fec_uref, 0, &size, &fec_header);
    uint64_t snbase_low = smpte_fec_get_snbase_low(fec_header);
    uint64_t ts_rec = smpte_fec_get_ts_recovery(fec_header);
    uint16_t length_rec = smpte_fec_get_length_rec(fec_header);
    uref_block_unmap(fec_uref, 0);

    // FIXME
#if 0
    if( upipe_rtp_fec->last_row_fec_snbase != UINT64_MAX )
    {
    uint64_t expected_row_fec_snbase;

    }
#endif

    uint64_t snbase_end_plus1 = snbase_low + upipe_rtp_fec->cols;
    uint64_t snbase_end = snbase_end_plus1 - 1;
    snbase_end &= UINT16_MAX;
    snbase_end_plus1 &= UINT16_MAX;
    uint16_t expected_seqnum = snbase_low;
    int drop_cnt = 0;

    upipe_rtp_fec->last_row_fec_snbase = snbase_low;

    printf("\n row processing %"PRIu64" \n", snbase_low);
    printf("\n row queuelen %i snbaselow %u length-rec %u \n", ulist_depth(&upipe_rtp_fec->main_queue), expected_seqnum, length_rec );

    for( int i = 0; i < upipe_rtp_fec->cols; i++ )
        seqnum_list[i] = expected_seqnum++;

    int processed = 0;
    uint64_t missing_seqnum = 0;
    ulist_foreach (&upipe_rtp_fec->main_queue, uchain) {
        struct uref *uref = uref_from_uchain(uchain);
        uref_rtp_get_seqnum(uref, &seqnum);

        //printf("\n row-fec checking %u \n", seqnum);

        for( int i = 0; i < upipe_rtp_fec->cols; i++ )
        {
            if( seqnum_list[i] == seqnum )
            {
                processed++;
                found_seqnum[i] = 1;
            }

            if( processed == upipe_rtp_fec->cols )
                break;
        }
    }

    printf("\n row processed %i \n", processed );
    /* Recoverable packet */
    if( processed == upipe_rtp_fec->cols-1 )
    {
        uint64_t missing_seqnum = 0;
        uint8_t *dst;

        ulist_foreach (&upipe_rtp_fec->main_queue, uchain) {
            struct uref *uref = uref_from_uchain(uchain);
            size_t uref_len = 0;
            uref_rtp_get_seqnum(uref, &seqnum);

            for( int i = 0; i < upipe_rtp_fec->cols; i++)
            {
                if( seqnum_list[i] == seqnum )
                {
                    uint64_t timestamp = 0;
                    uref_rtp_get_timestamp(uref, &timestamp);
                    uref_block_size(uref, &uref_len);
                    length_rec ^= uref_len;
                    printf("\n applying row length rec %u \n", uref_len );
                    ts_rec ^= timestamp;
                }
            }
        }

        if( length_rec != 1316 )
        {
            printf("\n DUBIOUS REC LEN %i \n", length_rec );
        }

        for( int i = 0; i < upipe_rtp_fec->cols; i++)
        {
            if( found_seqnum[i] == 0 )
            {
                missing_seqnum = seqnum_list[i];
                break;
            }
        }

        uref_block_resize(fec_uref, FEC_HEADER_SIZE, -1);
        uref_block_write(fec_uref, 0, &length_rec, &dst);

        processed = 0;
        ulist_foreach (&upipe_rtp_fec->main_queue, uchain) {
            struct uref *uref = uref_from_uchain(uchain);
            uint8_t *block;
            int size = -1;
            uref_rtp_get_seqnum(uref, &seqnum);

            for( int i = 0; i < upipe_rtp_fec->cols; i++)
            {
                if( seqnum_list[i] == seqnum )
                {
                    uref_block_read(uref, 0, &size, &block);
                    upipe_rtp_fec_xor_c( dst, block, size );
                    uref_block_unmap(uref, 0);
                    break;
                }
            }

            if( processed == upipe_rtp_fec->cols-1 )
                break;
        }
        printf("\n row correctheader %x not-lost seqnum %u timestamp %u \n", dst[0], missing_seqnum, ts_rec );
        uref_block_unmap(fec_uref, 0);
        uref_rtp_set_seqnum(fec_uref, missing_seqnum);
        uref_rtp_set_timestamp(fec_uref, ts_rec);

        insert_corrected_uref(upipe_rtp_fec, fec_uref);
    }

    return 0;
}

static int upipe_rtp_fec_apply_col_fec(struct upipe_rtp_fec *upipe_rtp_fec,
                                       struct uref *fec_uref)
{
    struct uchain *uchain, *uchain_tmp;
    uint64_t seqnum = 0;
    uint8_t *fec_header;
    size_t size = FEC_HEADER_SIZE;

    uint16_t seqnum_list[50];
    bool found_seqnum[50] = {0};

    uref_block_read(fec_uref, 0, &size, &fec_header);
    uint64_t snbase_low = smpte_fec_get_snbase_low(fec_header);
    uint64_t ts_rec = smpte_fec_get_ts_recovery(fec_header);
    uint16_t length_rec = smpte_fec_get_length_rec(fec_header);
    uref_block_unmap(fec_uref, 0);

    if( upipe_rtp_fec->last_matrix_snbase == UINT64_MAX &&
        seq_num_lt( upipe_rtp_fec->first_seqnum, snbase_low ) )
        upipe_rtp_fec->last_matrix_snbase = snbase_low;

    uint64_t snbase_end_plus1 = snbase_low + upipe_rtp_fec->cols;
    uint64_t snbase_end = snbase_end_plus1 - 1;
    snbase_end &= UINT16_MAX;
    snbase_end_plus1 &= UINT16_MAX;
    uint16_t expected_seqnum = snbase_low;
    int drop_cnt = 0;

    printf("\n col queuelen %i snbaselow %u \n", ulist_depth(&upipe_rtp_fec->main_queue), expected_seqnum );

    for( int i = 0; i < upipe_rtp_fec->rows; i++ )
    {
        seqnum_list[i] = expected_seqnum;
        expected_seqnum += upipe_rtp_fec->cols;
    }

    int processed = 0;
    ulist_foreach(&upipe_rtp_fec->main_queue, uchain) {
        struct uref *uref = uref_from_uchain(uchain);
        uref_rtp_get_seqnum(uref, &seqnum);

        for( int i = 0; i < upipe_rtp_fec->rows; i++)
        {
            if( seqnum_list[i] == seqnum )
            {
                processed++;
                found_seqnum[i] = 1;
            }

            if(processed == upipe_rtp_fec->rows)
                break;
        }
    }

    printf("\n col processed %i \n", processed );
    /* Recoverable packet */
    if( processed == upipe_rtp_fec->rows-1 )
    {
        uint64_t missing_seqnum = 0;
        uint8_t *dst;

        processed = 0;
        ulist_foreach (&upipe_rtp_fec->main_queue, uchain) {
            struct uref *uref = uref_from_uchain(uchain);
            size_t uref_len = 0;
            uref_rtp_get_seqnum(uref, &seqnum);

            for( int i = 0; i < upipe_rtp_fec->rows; i++)
            {
                if( seqnum_list[i] == seqnum )
                {
                    uint64_t timestamp = 0;
                    uref_rtp_get_timestamp(uref, &timestamp);
                    uref_block_size(uref, &uref_len);
                    length_rec ^= uref_len;
                    ts_rec ^= timestamp;
                    break;
                }
            }
            if( processed == upipe_rtp_fec->rows-1 )
                break;
        }

        if( length_rec != 1316 )
        {
            printf("\n DUBIOUS REC LEN %i \n", length_rec );
        }

        for( int i = 0; i < upipe_rtp_fec->rows; i++)
        {
            if( found_seqnum[i] == 0 )
            {
                missing_seqnum = seqnum_list[i];
                break;
            }
        }

        uref_block_resize(fec_uref, FEC_HEADER_SIZE, -1);
        uref_block_write(fec_uref, 0, &length_rec, &dst);
        printf("\n fecdataheader %x \n", dst[0] );

        processed = 0;
        ulist_foreach (&upipe_rtp_fec->main_queue, uchain) {
            struct uref *uref = uref_from_uchain(uchain);
            uint8_t *block;
            int size = -1;
            uref_rtp_get_seqnum(uref, &seqnum);

            for( int i = 0; i < upipe_rtp_fec->rows; i++)
            {
                if( seqnum_list[i] == seqnum )
                {
                    uref_block_read(uref, 0, &size, &block);
                    upipe_rtp_fec_xor_c( dst, block, size );
                    uref_block_unmap(uref, 0);
                    processed++;
                    break;
                }

                if( processed == upipe_rtp_fec->rows-1 )
                    break;
            }
        }
        printf("\n col correctheader %x not-lost seqnum %u timestamp %u \n", dst[0], missing_seqnum, ts_rec );
        uref_block_unmap(fec_uref, 0);
        uref_rtp_set_seqnum(fec_uref, missing_seqnum);
        uref_rtp_set_timestamp(fec_uref, ts_rec);

        insert_corrected_uref(upipe_rtp_fec, fec_uref);
    }

    return 0;
}

static void upipe_rtp_fec_clear(struct upipe_rtp_fec *upipe_rtp_fec)
{
    struct uchain *uchain, *uchain_tmp;
    ulist_delete_foreach (&upipe_rtp_fec->main_queue, uchain, uchain_tmp) {
        struct uref *uref = uref_from_uchain(uchain);
        ulist_delete(uchain);
        uref_free(uref);
    }

    ulist_delete_foreach (&upipe_rtp_fec->row_queue, uchain, uchain_tmp) {
        struct uref *uref = uref_from_uchain(uchain);
        ulist_delete(uchain);
        uref_free(uref);
    }
}

static void upipe_rtp_fec_timer(struct upump *upump)
{
    struct upipe *upipe = upump_get_opaque(upump, struct upipe *);
    struct upipe_rtp_fec *upipe_rtp_fec = upipe_rtp_fec_from_upipe(upipe);
    struct uref *uref_out;


}

/** @internal @This receives a provided ubuf manager.
 *
 * @param upipe description structure of the pipe
 * @param flow_format amended flow format
 * @return an error code
 */
static int upipe_rtp_fec_check(struct upipe *upipe, struct uref *flow_format)
{
    struct upipe_rtp_fec *upipe_rtp_fec = upipe_rtp_fec_from_upipe(upipe);
    if (flow_format != NULL)
        upipe_rtp_fec_store_flow_def(upipe, flow_format);

    if (upipe_rtp_fec->flow_def == NULL)
        return UBASE_ERR_NONE;

    if (upipe_rtp_fec->uref_mgr == NULL) {
        upipe_rtp_fec_require_uref_mgr(upipe);
        return UBASE_ERR_NONE;
    }

    // FIXME this is broke

    return UBASE_ERR_NONE;
}

/** @internal @This handles input uref.
 *
 * @param upipe description structure of the pipe
 * @param uref uref structure
 * @param upump_p reference to upump structure
 */
static void upipe_rtp_fec_sub_input(struct upipe *upipe, struct uref *uref,
                                    struct upump **upump_p)
{
    struct upipe_rtp_fec *upipe_rtp_fec =
        upipe_rtp_fec_from_sub_mgr(upipe->mgr);
    struct upipe_rtp_fec_sub *upipe_rtp_fec_sub =
        upipe_rtp_fec_sub_from_upipe(upipe);
    uint8_t fec_buffer[FEC_HEADER_SIZE];
    int fec_change = 0;

    // if discontinuity reset everything

    if( upipe_rtp_fec_sub == &upipe_rtp_fec->main_subpipe )
    {
        uint64_t seqnum;
        uref_rtp_get_seqnum(uref, &seqnum);
        uint64_t timestamp;
        uref_rtp_get_timestamp(uref, &timestamp);

        if( upipe_rtp_fec->first_seqnum == UINT64_MAX )
        {
            upipe_rtp_fec->first_seqnum = seqnum;
            printf("\n FIRST seqnum %"PRIu64" \n", seqnum );
        }
        upipe_rtp_fec->last_seqnum = seqnum;

        printf("\n arrived %"PRIu64" ts %"PRIu64" \n", seqnum, timestamp);

        ulist_add(&upipe_rtp_fec->main_queue, uref_to_uchain(uref));

        if( upipe_rtp_fec->cols && upipe_rtp_fec->rows )
        {
            uint64_t last_row_fec_snbase = upipe_rtp_fec->last_row_fec_snbase == UINT64_MAX ? upipe_rtp_fec->first_seqnum :
                                           upipe_rtp_fec->last_row_fec_snbase;

            uint64_t delta = (seqnum + UINT16_MAX - last_row_fec_snbase) & UINT16_MAX;
            if( delta > 2*upipe_rtp_fec->cols )
            {
                //printf("APPLY ROW delta %u queuelen %u \n", delta, ulist_depth(&upipe_rtp_fec->main_queue) );
                upipe_rtp_fec_apply_row_fec(upipe_rtp_fec, last_row_fec_snbase);
            }
        }

        if( !upipe_rtp_fec->cols && !upipe_rtp_fec->rows )
        {
            // FIXME if none sent packet
        }

        upipe_rtp_fec->pkts_since_last_fec++;
    }
    else
    {
        const uint8_t *fec_header = uref_block_peek(uref, 0, FEC_HEADER_SIZE, fec_buffer);
        uint8_t d = smpte_fec_check_d(fec_header);
        uint8_t offset = smpte_fec_get_offset(fec_header);
        uint8_t na = smpte_fec_get_na(fec_header);
        uref_block_peek_unmap(uref, 0, fec_buffer, fec_header);

        upipe_rtp_fec->pkts_since_last_fec = 0;

        if( upipe_rtp_fec_sub == &upipe_rtp_fec->col_subpipe )
        {
            if( d )
            {
                // INVALID
                uref_free(uref);
                return;
            }
            if( upipe_rtp_fec->cols != offset )
            {
                upipe_rtp_fec->cols = offset;
                upipe_rtp_fec->rows = na;
                fec_change = 1;
            }
            upipe_rtp_fec_apply_col_fec(upipe_rtp_fec, uref);
        }
        else if( upipe_rtp_fec_sub == &upipe_rtp_fec->row_subpipe )
        {
            if( !d )
            {
                // INVALID
                uref_free(uref);
                return;
            }
            ulist_add(&upipe_rtp_fec->row_queue, uref_to_uchain(uref));
        }
    }

    if( upipe_rtp_fec->pkts_since_last_fec > 200 && (upipe_rtp_fec->rows || upipe_rtp_fec->cols) )
    {
        upipe_rtp_fec->rows = upipe_rtp_fec->cols = 0;
        fec_change = 1;
    }

    if( fec_change )
    {
         // clear
        upipe_rtp_fec_clear(upipe_rtp_fec);
        fec_change = 0;

        printf("\n \n \n \n FEC Change %i %i \n \n \n \n", upipe_rtp_fec->cols, upipe_rtp_fec->rows);
    }
}

/** @internal @This sets the input flow definition.
 *
 * @param upipe description structure of the pipe
 * @param flow_def flow definition packet
 * @return an error code
 */
static int upipe_rtp_fec_sub_set_flow_def(struct upipe *upipe,
                                          struct uref *flow_def)
{
    if (flow_def == NULL)
        return UBASE_ERR_INVALID;

    struct upipe_rtp_fec *upipe_rtp_fec =
        upipe_rtp_fec_from_sub_mgr(upipe->mgr);
    struct upipe_rtp_fec_sub *upipe_rtp_fec_sub =
        upipe_rtp_fec_sub_from_upipe(upipe);

    return UBASE_ERR_NONE;
}

/** @internal @This processes control commands on an output subpipe of an
 * upipe_rtp_fec pipe.
 *
 * @param upipe description structure of the pipe
 * @param command type of command to process
 * @param args arguments of the command
 * @return an error code
 */
static int upipe_rtp_fec_sub_control(struct upipe *upipe,
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
            return upipe_rtp_fec_sub_set_flow_def(upipe, flow_def);
        }
        case UPIPE_SUB_GET_SUPER: {
            struct upipe **p = va_arg(args, struct upipe **);
            *p = upipe_rtp_fec_to_upipe(upipe_rtp_fec_from_sub_mgr(upipe->mgr));
        }

        default:
            return UBASE_ERR_UNHANDLED;
    }
}

/** @This cleans a upipe.
 *
 * @param upipe description structure of the pipe
 */
static void upipe_rtp_fec_sub_clean(struct upipe *upipe)
{
    struct upipe_rtp_fec *upipe_rtp_fec =
        upipe_rtp_fec_from_sub_mgr(upipe->mgr);
    upipe_throw_dead(upipe);

    // FIXME

    upipe_clean(upipe);
}

/** @internal @This initializes the output manager for an upipe_rtp_fec pipe.
 *
 * @param upipe description structure of the pipe
 */
static void upipe_rtp_fec_init_sub_mgr(struct upipe *upipe)
{
    struct upipe_rtp_fec *upipe_rtp_fec = upipe_rtp_fec_from_upipe(upipe);
    struct upipe_mgr *sub_mgr = &upipe_rtp_fec->sub_mgr;
    sub_mgr->refcount = upipe_rtp_fec_to_urefcount(upipe_rtp_fec);
    sub_mgr->signature = UPIPE_RTP_FEC_INPUT_SIGNATURE;
    sub_mgr->upipe_alloc = NULL;
    sub_mgr->upipe_input = upipe_rtp_fec_sub_input;
    sub_mgr->upipe_control = upipe_rtp_fec_sub_control;
    sub_mgr->upipe_mgr_control = NULL;
}

/** @internal @This builds the flow definition packet.
 *
 * @param upipe description structure of the pipe
 */
static void upipe_rtp_fec_build_flow_def(struct upipe *upipe)
{
    struct upipe_rtp_fec *upipe_rtp_fec = upipe_rtp_fec_from_upipe(upipe);
    if (upipe_rtp_fec->uref_mgr == NULL)
        return;

    struct uref *flow_def =
        uref_block_flow_alloc_def(upipe_rtp_fec->uref_mgr, "block.mpegtsaligned.");
    if (unlikely(flow_def == NULL)) {
        upipe_throw_fatal(upipe, UBASE_ERR_ALLOC);
        return;
    }

#if 0
    if (upipe_rtp_fec->latency)
        UBASE_FATAL(upipe, uref_clock_set_latency(flow_def,
                                                  upipe_rtp_fec->latency))
#endif
    upipe_rtp_fec_store_flow_def(upipe, flow_def);
}

/** @internal @This allocates a rtp-fec pipe.
 *
 * @param mgr common management structure
 * @param uprobe structure used to raise events
 * @param signature signature of the pipe allocator
 * @param args optional arguments
 * @return pointer to upipe or NULL in case of allocation error
 */
static struct upipe *_upipe_rtp_fec_alloc(struct upipe_mgr *mgr,
                                          struct uprobe *uprobe,
                                          uint32_t signature, va_list args)
{
    if (signature != UPIPE_RTP_FEC_SIGNATURE)
        return NULL;
    struct uprobe *uprobe_main = va_arg(args, struct uprobe *);
    struct uprobe *uprobe_col  = va_arg(args, struct uprobe *);
    struct uprobe *uprobe_row  = va_arg(args, struct uprobe *);

    struct upipe_rtp_fec *upipe_rtp_fec =
        (struct upipe_rtp_fec *)calloc(1, sizeof(struct upipe_rtp_fec));
    if (unlikely(upipe_rtp_fec == NULL)) {
        uprobe_release(uprobe_main);
        uprobe_release(uprobe_col);
        uprobe_release(uprobe_row);
        return NULL;
    }

    upipe_rtp_fec->first_seqnum = UINT64_MAX;
    upipe_rtp_fec->last_matrix_snbase = UINT64_MAX;
    upipe_rtp_fec->last_row_fec_snbase = UINT64_MAX;

    upipe_rtp_fec->pump_start = 0;
    upipe_rtp_fec->first_exit_time = UINT64_MAX;
    upipe_rtp_fec->first_timestamp = UINT64_MAX;
    upipe_rtp_fec->last_timestamp = UINT64_MAX;
    upipe_rtp_fec->last_timestamp_raw = UINT64_MAX;

    struct upipe *upipe = upipe_rtp_fec_to_upipe(upipe_rtp_fec);
    upipe_init(upipe, mgr, uprobe);

    upipe_rtp_fec_init_urefcount(upipe);
    upipe_rtp_fec_init_uref_mgr(upipe);
    upipe_rtp_fec_init_sub_mgr(upipe);
    upipe_rtp_fec_init_output(upipe);

    /* Initalise subpipes */
    upipe_rtp_fec_sub_init(upipe_rtp_fec_sub_to_upipe(upipe_rtp_fec_to_main_subpipe(upipe_rtp_fec)),
                            &upipe_rtp_fec->sub_mgr, uprobe_main);
    upipe_rtp_fec_sub_init(upipe_rtp_fec_sub_to_upipe(upipe_rtp_fec_to_col_subpipe(upipe_rtp_fec)),
                            &upipe_rtp_fec->sub_mgr, uprobe_col);
    upipe_rtp_fec_sub_init(upipe_rtp_fec_sub_to_upipe(upipe_rtp_fec_to_row_subpipe(upipe_rtp_fec)),
                            &upipe_rtp_fec->sub_mgr, uprobe_row);

    ulist_init(&upipe_rtp_fec->main_queue);
    ulist_init(&upipe_rtp_fec->row_queue);

    upipe_throw_ready(upipe);

    return upipe;
}

/** @internal @This processes control commands on a file source pipe, and
 * checks the status of the pipe afterwards.
 *
 * @param upipe description structure of the pipe
 * @param command type of command to process
 * @param args arguments of the command
 * @return an error code
 */
static int upipe_rtp_fec_control(struct upipe *upipe, int command, va_list args)
{
    switch (command) {
        /* generic commands */
        case UPIPE_REGISTER_REQUEST: {
            struct urequest *request = va_arg(args, struct urequest *);
            return upipe_throw_provide_request(upipe, request);
        }
        case UPIPE_UNREGISTER_REQUEST:
            return UBASE_ERR_NONE;
        case UPIPE_GET_FLOW_DEF: {
            struct uref **p = va_arg(args, struct uref **);
            return upipe_rtp_fec_get_flow_def(upipe, p);
        }
        case UPIPE_GET_OUTPUT: {
            struct upipe **p = va_arg(args, struct upipe **);
            return upipe_rtp_fec_get_output(upipe, p);
        }
        case UPIPE_SET_OUTPUT: {
            struct upipe *output = va_arg(args, struct upipe *);
            return upipe_rtp_fec_set_output(upipe, output);
        }
        /* specific commands */
        case UPIPE_RTP_FEC_GET_MAIN_SUB: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_RTP_FEC_SIGNATURE)
            struct upipe **upipe_p = va_arg(args, struct upipe **);
            *upipe_p =  upipe_rtp_fec_sub_to_upipe(
                            upipe_rtp_fec_to_main_subpipe(
                                upipe_rtp_fec_from_upipe(upipe)));
            return UBASE_ERR_NONE;
        }
        case UPIPE_RTP_FEC_GET_COL_SUB: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_RTP_FEC_SIGNATURE)
            struct upipe **upipe_p = va_arg(args, struct upipe **);
            *upipe_p =  upipe_rtp_fec_sub_to_upipe(
                            upipe_rtp_fec_to_col_subpipe(
                                upipe_rtp_fec_from_upipe(upipe)));
            return UBASE_ERR_NONE;
        }
        case UPIPE_RTP_FEC_GET_ROW_SUB: {
            UBASE_SIGNATURE_CHECK(args, UPIPE_RTP_FEC_SIGNATURE)
            struct upipe **upipe_p = va_arg(args, struct upipe **);
            *upipe_p =  upipe_rtp_fec_sub_to_upipe(
                            upipe_rtp_fec_to_row_subpipe(
                                upipe_rtp_fec_from_upipe(upipe)));
            return UBASE_ERR_NONE;
        }
        default:
            return UBASE_ERR_UNHANDLED;
    }
}

/** @This frees a upipe.
 *
 * @param upipe description structure of the pipe
 */
static void upipe_rtp_fec_free(struct upipe *upipe)
{
    struct upipe_rtp_fec *upipe_rtp_fec = upipe_rtp_fec_from_upipe(upipe);


    upipe_throw_dead(upipe);

    upipe_rtp_fec_clean_urefcount(upipe);
    upipe_rtp_fec_clean_uref_mgr(upipe);

    upipe_rtp_fec_clean_output(upipe);
    upipe_rtp_fec_clean_urefcount(upipe);
    //upipe_rtp_fec_free_flow(upipe);
}

/** module manager static descriptor */
static struct upipe_mgr upipe_rtp_fec_mgr = {
    .refcount = NULL,
    .signature = UPIPE_RTP_FEC_SIGNATURE,

    .upipe_alloc = _upipe_rtp_fec_alloc,
    .upipe_input = NULL,
    .upipe_control = upipe_rtp_fec_control,

    .upipe_mgr_control = NULL
};

/** @This returns the management structure for rtp-fec pipes
 *
 * @return pointer to manager
 */
struct upipe_mgr *upipe_rtp_fec_mgr_alloc(void)
{
    return &upipe_rtp_fec_mgr;
}

