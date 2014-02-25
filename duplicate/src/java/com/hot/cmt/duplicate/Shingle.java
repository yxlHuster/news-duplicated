package com.hot.cmt.duplicate;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * 查重服务，用来计算文章内容的shingles
 * @author yongleixiao
 *
 */
public class Shingle {

    private TTransport transport = null;
    private TProtocol protocol = null;
    private shingleService.Client client = null;

    private String ip = null;
    private int port = 0;
    private int portNum = 0;
    /* 超时等待时间 */
    private int timeOut = 5;
    /* 重试次数 */
    private int tryTimes = 3;

    public Shingle(String ip, int port, int portNum) throws TTransportException {
        this.ip = ip;
        this.port = port;
        this.portNum = portNum;
        open(ip, port, portNum);
    }

    public void open(String ip, int port, int portNum) throws TTransportException {
        int p = (int) (Math.random() * portNum) + port;
        transport = new TSocket(ip, p);
        protocol = new GBKCompactProtocol(transport);
        client = new shingleService.Client(protocol);
        try {
            transport.open();
        } catch (TTransportException tt) {
            try {
                Thread.sleep(timeOut);
                transport.open();
            } catch (InterruptedException ie) {}
        }
    }

    public List<String> getShingleString(String content) throws TException {
        List<String> shingles = new ArrayList<String>();
        for (int i = 0; i < tryTimes; i++) {
            try {
                shingles = client.getShingleString(content);
                return shingles;
            } catch (TException ex) {
                try {
                    Thread.sleep(timeOut);
                    if (i == 1) {
                        close();
                        open(this.ip, this.port, this.portNum);
                    }
                } catch (InterruptedException e) {}
            }
        }
        return shingles;
    }

    public void close() {
        transport.close();
    }
}
