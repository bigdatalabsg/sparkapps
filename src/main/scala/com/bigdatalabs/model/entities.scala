package com.bigdatalabs.model

object entities {

  case class cTrade(
                    xchange: String,
                    symbol: String,
                    trdate: String,
                    open: Float,
                    high: Float,
                    low: Float,
                    close: Float,
                    volume: Integer,
                    adj_close: Float
                   ) extends Serializable
}