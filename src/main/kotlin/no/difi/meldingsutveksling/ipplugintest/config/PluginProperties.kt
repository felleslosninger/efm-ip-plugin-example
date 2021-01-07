package no.difi.meldingsutveksling.ipplugintest.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "no.difi")
class PluginProperties {

}