package de.dws.berlin.functions;


import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sagemaker.AmazonSageMaker;
import com.amazonaws.services.sagemaker.AmazonSageMakerClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntime;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntimeClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.ExecutorUtils;

public class TranslateAsyncFunction extends RichAsyncFunction<String[], String[]> {

  private static final long serialVersionUID = 2098635244857937717L;

  private static final String SERVICE_ENDPOINT = "https://runtime.sagemaker.us-east-1.amazonaws.com/endpoints/de-2-en/invocations";

  private transient ExecutorService executorService;

  private final long shutdownWaitTS;

  private AmazonSageMakerRuntime amazonSageMakerRuntime;

  private void getSageMakerClient() {
     amazonSageMakerRuntime = AmazonSageMakerRuntimeClient.builder()
        .withRegion(Regions.US_EAST_1)
        .withCredentials(new ProfileCredentialsProvider("flink-sagemaker"))
        .withEndpointConfiguration(
            new AwsClientBuilder
                .EndpointConfiguration(SERVICE_ENDPOINT, Regions.US_EAST_1.getName()))
        .build();
  }

  public TranslateAsyncFunction(long shutdownWaitTS) {
    this.shutdownWaitTS = shutdownWaitTS;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    executorService = Executors.newFixedThreadPool(30);
    getSageMakerClient();
  }

  @Override
  public void close() throws Exception {
    super.close();
    ExecutorUtils.gracefulShutdown(shutdownWaitTS, TimeUnit.MILLISECONDS, executorService);
  }

  @Override
  public void asyncInvoke(String[] string, ResultFuture<String[]> resultFuture) throws Exception {


  }
}
