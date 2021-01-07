package no.difi.meldingsutveksling.ipplugintest

import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.servicebus.ClientFactory
import com.microsoft.azure.servicebus.IMessageSender
import com.microsoft.azure.servicebus.Message
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder
import no.difi.meldingsutveksling.api.AsicHandler
import no.difi.meldingsutveksling.api.DpeConversationStrategy
import no.difi.meldingsutveksling.nextmove.NextMoveOutMessage
import no.difi.meldingsutveksling.pipes.Reject
import no.difi.meldingsutveksling.util.logger
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

@Component
@Order(1)
class TestPlugin(val objectMapper: ObjectMapper, val asicHandler: AsicHandler) : DpeConversationStrategy {

    val log = logger()

    @Value("\${difi.move.plugin.blob-storage-url}")
    lateinit var blobStorageUrl: String

    @Value("\${difi.move.plugin.servicebus-url}")
    lateinit var serviceBusUrl: String

    lateinit var messageSender: IMessageSender
    lateinit var blobClient: BlobServiceClient
    lateinit var blobContainerClient: BlobContainerClient

    @PostConstruct
    fun init() {
        val csb = ConnectionStringBuilder(serviceBusUrl, "testqueue")
        messageSender = ClientFactory.createMessageSenderFromConnectionStringBuilder(csb)

        blobClient = BlobServiceClientBuilder().connectionString(blobStorageUrl).buildClient();
        blobContainerClient = blobClient.getBlobContainerClient("testcontainer")
    }

    override fun send(message: NextMoveOutMessage) {
        log.info("Sending message with messageId=${message.messageId}")

        if (message.files.isNotEmpty()) {
            val asicStream =
                asicHandler.createEncryptedAsic(message, Reject { t -> log.error("Error creating ASiC", t) })
            val asicBytes = asicStream.readBytes()

            val filename = "${message.messageId}-asic.zip"
            val blobClient = blobContainerClient.getBlobClient(filename)
            log.info("Uploading file with name=${filename}, size=${asicBytes.size} to ${blobClient.blobUrl}")
            blobClient.upload(asicBytes.inputStream(), asicBytes.size.toLong(), true)
            log.info("Upload done..")
        }

        val payload = objectMapper.writeValueAsBytes(message)
        messageSender.send(Message(message.messageId, payload, null))
        log.info("Message with id=${message.messageId} sent")
    }
}
