package org.acme.edge;

import javax.enterprise.context.ApplicationScoped;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;



import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class Routes extends RouteBuilder {
    
    @Override
    public void configure() throws Exception {
        
        // opcua client
        from("milo-client:opc.tcp://{{opcuaserver.host}}:{{opcuaserver.port}}/milo?allowedSecurityPolicies=None&samplingInterval=500&node=RAW(ns=2;s=Dynamic/RandomInt32)")
                .routeId("FromOPCUA2Payload")
                .log("Received : \"${body}\"")
                .process(exchange -> {
                    DataValue data = exchange.getIn().getBody(DataValue.class);
                    exchange.setProperty("Status", data.getStatusCode().toString());
                    exchange.setProperty("Value", data.getValue().getValue());
                    exchange.setProperty("Time", data.getSourceTime().toString());
                })
                .to("direct:sink");
        
        from("direct:sink")
            .routeId("FromPayload2Msg")
            .multicast()
                .to("direct:amqp")
                .to("direct:kafka")
                .to("direct:aws");
        // enrich message
        from("direct:kafka")
            .routeId("FromMsg2Kafka")
            .setBody().simple("${exchangeProperty.Status}")
            .to("kafka:{{kafka.topic.name}}")
            .log("Message sent correctly to KAFKA! : \"${body}\" ");
        // filter message
        from("direct:amqp")
            .routeId("FromMsg2AMQ")
            .setBody().simple("${exchangeProperty.Value}")
            .to("paho:{{mqtt.topic.name}}?brokerUrl=tcp://{{mqtt.server}}:{{mqtt.port}}")
            .log("Message sent correctly AMQ-BROKER! : \"${body}\" ");
        
        // mask message
        from("direct:aws")
            .routeId("FromMsg2Kinesis")
            .setBody().simple("${exchangeProperty.Time}")
            .to("aws2-kinesis://{{aws.kinesis.stream-name}}?useDefaultCredentialsProvider=true&region=eu-central-1")
            .log("Message sent correctly to KINESIS! : \"${body}\" "); 
    }
}
