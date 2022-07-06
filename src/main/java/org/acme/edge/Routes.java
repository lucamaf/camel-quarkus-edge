package org.acme.edge;

import javax.enterprise.context.ApplicationScoped;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;



import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class Routes extends RouteBuilder {
    
    @Override
    public void configure() throws Exception {
        
        // opcua client
        from("milo-client:opc.tcp://{{opcuaserver.host}}:{{opcuaserver.port}}/milo?allowedSecurityPolicies=None&samplingInterval=5000&node=RAW(ns=2;s=Dynamic/RandomInt32)")
                .routeId("FromOPCUA2Payload")
                .log("Received : \"${body}\"")
                .process(exchange -> {
                    DataValue data = exchange.getIn().getBody(DataValue.class);
                    exchange.setProperty("Status", data.getStatusCode().toString());
                    exchange.setProperty("Value", data.getValue().getValue().toString());
                    exchange.setProperty("Time", data.getSourceTime().toString());
                })
                .to("direct:sink");
        
        from("direct:sink")
            .routeId("FromPayload2Msg")
            .multicast()
                .to("direct:aws")
                .to("direct:kafka")
                .to("direct:mqtt");
        // enrich message
        // add progressive id to msg
        from("direct:kafka")
            .routeId("FromMsg2Kafka")
            .setBody().simple("${exchangeProperty.Status}")
            .log("${exchangeProperty.Status}")
            .to("kafka:{{kafka.topic.name}}")
            .log("Message sent correctly to KAFKA! : \"${body}\" ");
        // filter message
        // send only if value is positive
        from("direct:mqtt")
            .routeId("FromMsg2AMQ")
            .setBody().simple("${exchangeProperty.Status}")
            .log("${exchangeProperty.Value}")
            .to("paho:{{mqtt.topic.name}}?brokerUrl=tcp://{{mqtt.server}}:{{mqtt.port}}")
            .log("Message sent correctly AMQ-BROKER! : \"${body}\" ");
        
        // mask message
        // asterisk the value
        from("direct:aws")
            .routeId("FromMsg2Kinesis")
            .setBody().simple("${exchangeProperty.Status}")
            .log("${exchangeProperty.Time}")
            .setHeader("CamelAwsKinesisPartitionKey", constant(0))
            .to("aws2-kinesis://{{aws.kinesis.stream-name}}?aws.accessKey={{aws.access.key.id}}&secretKey={{aws.secret.access.key}}&region=eu-central-1")
            .log("Message sent correctly to KINESIS! : \"${body}\" "); 
    }
}
