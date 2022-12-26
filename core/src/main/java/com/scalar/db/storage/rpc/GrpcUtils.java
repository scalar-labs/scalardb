package com.scalar.db.storage.rpc;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;

public final class GrpcUtils {

  private GrpcUtils() {}

  public static ManagedChannel createChannel(GrpcConfig config) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forAddress(config.getHost(), config.getPort()).usePlaintext();
    config.getMaxInboundMessageSize().ifPresent(builder::maxInboundMessageSize);
    config.getMaxInboundMetadataSize().ifPresent(builder::maxInboundMetadataSize);
    return builder.build();
  }
}
