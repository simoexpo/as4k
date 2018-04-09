package org.simoexpo.as4k.it.testing

import org.scalatest.concurrent.{AbstractPatienceConfiguration, PatienceConfiguration}
import org.scalatest.time.{Millis, Seconds, Span}

trait LooseIntegrationPatience extends AbstractPatienceConfiguration { this: PatienceConfiguration =>

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = scaled(Span(20, Seconds)),
      interval = scaled(Span(200, Millis))
    )

}
