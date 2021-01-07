package no.difi.meldingsutveksling.ipplugintest

import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.servicebus.ClientFactory
import com.microsoft.azure.servicebus.IMessageReceiver
import com.microsoft.azure.servicebus.ReceiveMode
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import no.difi.meldingsutveksling.ServiceIdentifier.DPE
import no.difi.meldingsutveksling.api.DpePolling
import no.difi.meldingsutveksling.api.NextMoveQueue
import no.difi.meldingsutveksling.nextmove.NextMoveInMessage
import no.difi.meldingsutveksling.util.logger
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.time.Duration
import javax.annotation.PostConstruct

@Component
@Order(1)
class TestPoller(val objectMapper: ObjectMapper, val nextMoveQueue: NextMoveQueue) : DpePolling {

    val log = logger()

    @Value("\${difi.move.plugin.blob-storage-url}")
    lateinit var blobStorageUrl: String

    @Value("\${difi.move.plugin.servicebus-url}")
    lateinit var serviceBusUrl: String

    lateinit var messageReceiver: IMessageReceiver
    lateinit var blobClient: BlobServiceClient
    lateinit var blobContainerClient: BlobContainerClient

    @PostConstruct
    fun init() {
        val csb = ConnectionStringBuilder(serviceBusUrl, "testqueue")
        messageReceiver = ClientFactory.createMessageReceiverFromConnectionStringBuilder(csb, ReceiveMode.PEEKLOCK)

        blobClient = BlobServiceClientBuilder().connectionString(blobStorageUrl).buildClient()
        blobContainerClient = blobClient.getBlobContainerClient("testcontainer")
    }

    override fun poll() {
        log.info("Polling for new messages..")
        messageReceiver.receiveBatch(50, Duration.ofSeconds(10))?.forEach { m ->
            log.info("Processing message with messageId=${m.messageId}")
            val msg = objectMapper.readValue(m.messageBody.binaryData[0], NextMoveInMessage::class.java)

            if (!msg.files.isNullOrEmpty()) {
                val filename = "${msg.messageId}-asic.zip"
                log.info("Downloading file $filename from blob storage..")
                val fileClient = blobContainerClient.getBlobClient(filename)

                val pos = PipedOutputStream()
                val pis = PipedInputStream(pos)
                GlobalScope.launch {
                    pos.use { fileClient.download(it) }
                }

                nextMoveQueue.enqueueIncomingMessage(msg.sbd, DPE, pis)
                fileClient.delete()
            } else {
                nextMoveQueue.enqueueIncomingStatus(msg.sbd, DPE)
            }

            messageReceiver.complete(m.lockToken)
            log.info("Message processing done.")
        }
    }

}