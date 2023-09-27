package lds.bigdata.util

import com.typesafe.config.{Config, ConfigFactory}

object ConfigUtils {

    val config: Config = ConfigFactory.load()
    val flag: Boolean = config.getBoolean("local.run")
    val url = config.getString("url")
    val driverClass = config.getString("driverClass")
    val username = config.getString("username")
    val password = config.getString("password")
}
