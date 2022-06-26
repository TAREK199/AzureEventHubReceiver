import com.azure.messaging.eventhubs.*;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.*;
import com.azure.storage.blob.*;

import java.util.function.Consumer;

public class Receiver{

    private static final String connectionString = "Endpoint=sb://nbs-monitor-prod-evh-ns.servicebus.windows.net/;SharedAccessKeyName=authorization-0;SharedAccessKey=H18C3NMQOpH/XoQGS7MavvzKmOAopsA77nFBR0IGwRs=;EntityPath=nbs-monitor-prod-evh";
    private static final String eventHubName = "nbs-monitor-prod-evh";
    private static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=mystoragetarek;AccountKey=fEGlFnKHmti7kDBhyseuLdg+yq1oPxh2faL8gB5hio4Jghe+602THgeRAZcCM5PYmizr/0/5GiKT+AStF/v0+A==;EndpointSuffix=core.windows.net";
    private static final String storageContainerName = "container-test";



    public static final Consumer<EventContext> PARTITION_PROCESSOR = eventContext -> {
        PartitionContext partitionContext = eventContext.getPartitionContext();
        EventData eventData = eventContext.getEventData();

        System.out.printf("Processing event from partition %s with sequence number %d with body: %s%n",
                partitionContext.getPartitionId(), eventData.getSequenceNumber(), eventData.getBodyAsString());

        // Every 10 events received, it will update the checkpoint stored in Azure Blob Storage.
        if (eventData.getSequenceNumber() % 10 == 0) {
            eventContext.updateCheckpoint();
        }
    };

    public static final Consumer<ErrorContext> ERROR_HANDLER = errorContext -> {
        System.out.printf("Error occurred in partition processor for partition %s, %s.%n",
                errorContext.getPartitionContext().getPartitionId(),
                errorContext.getThrowable());
    };


    public static void main(String[] args) throws Exception {
        // Create a blob container client that you use later to build an event processor client to receive and process events
        BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
                .connectionString(storageConnectionString)
                .containerName(storageContainerName)
                .buildAsyncClient();

        // Create a builder object that you will use later to build an event processor client to receive and process events and errors.
        EventProcessorClientBuilder eventProcessorClientBuilder = new EventProcessorClientBuilder()
                .connectionString(connectionString, eventHubName)
                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .processEvent(PARTITION_PROCESSOR)
                .processError(ERROR_HANDLER)
                .checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient));

        // Use the builder object to create an event processor client
        EventProcessorClient eventProcessorClient = eventProcessorClientBuilder.buildEventProcessorClient();

        System.out.println("Starting event processor");
        eventProcessorClient.start();

        System.out.println("Press enter to stop.");
        System.in.read();

        System.out.println("Stopping event processor");
        eventProcessorClient.stop();
        System.out.println("Event processor stopped.");

        System.out.println("Exiting process");
    }

}
