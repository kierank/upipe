#ifndef _UPIPE_MODULES_RTP_VC2_UNPACK_H_
# define _UPIPE_MODULES_RTP_VC2_UNPACK_H_

#ifdef __cplusplus
extern "C" {
#endif

#define UPIPE_RTP_VC2_UNPACK_SIGNATURE UBASE_FOURCC('r','v','2','d')
struct upipe_mgr *upipe_rtp_vc2_unpack_mgr_alloc(void);

#ifdef __cplusplus
}
#endif

#endif
