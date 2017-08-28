#ifndef _UPIPE_MODULES_RTP_VC2_PACK_H_
# define _UPIPE_MODULES_RTP_VC2_PACK_H_

#ifdef __cplusplus
extern "C" {
#endif

#define UPIPE_RTP_VC2_PACK_SIGNATURE UBASE_FOURCC('r','v','2','e')
struct upipe_mgr *upipe_rtp_vc2_pack_mgr_alloc(void);

#ifdef __cplusplus
}
#endif

#endif
