package com.wyh.flink.utils;

import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author: WangYuhang
 * @create: 2020-05-29 15:40
 **/
public class SSH2Util {

    private static Logger logger = LoggerFactory.getLogger(SSH2Util.class);

    /**
     * 连接sftp服务器
     *
     * @throws Exception
     */
    public static ChannelSftp getSftpChanne(String host, int port, String user, String password) {

//        String host = "114.251.165.203";
//        Integer port = 2002;
//        String user = "feichengtianchen";
//        String password = "feichengtianchen";

        JSch jsch = new JSch();
        Session sshSession;
        ChannelSftp sftp = null;

        try {
            // 添加私钥路径
//            jsch.addIdentity(priKeyPath);
            sshSession = jsch.getSession(user, host, port);
            // 服务器之间配置免密登陆，不需要密码
            sshSession.setPassword(password);
            Properties sshConfig = new Properties();
            sshConfig.put("StrictHostKeyChecking", "no");
            sshSession.setConfig(sshConfig);
            sshSession.connect(60000);

            Channel channel = sshSession.openChannel("sftp");
            channel.connect();
            sftp = (ChannelSftp) channel;
        } catch (JSchException e) {
            logger.error("获取sftp连接失败，服务器地址为" + host);
            logger.error(e.getMessage());
        }
        return sftp;
    }

    /**
     * 关闭ssh连接
     * Disconnect with server
     *
     * @throws Exception
     */
    public static void disconnect(ChannelSftp sftp) {
        Session session = null;
        if (sftp != null) {
            try {
                session = sftp.getSession();
            } catch (JSchException e) {
                logger.error(e.getMessage());
            }
            if (sftp.isConnected()) {
                sftp.disconnect();
            }
            if (session != null && session.isConnected()) {
                session.disconnect();
            }
        }
        logger.info("ssh连接以关闭");
    }


}
