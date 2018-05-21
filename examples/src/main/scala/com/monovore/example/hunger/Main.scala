package com.monovore.example.hunger

import java.net.InetSocketAddress

import cats.effect.IO
import com.monovore.decline.{CommandApp, Opts}
import com.monovore.hunger
import com.monovore.hunger.{AsyncClient, GroupClient}
import org.apache.kafka.clients.consumer.RoundRobinAssignor
import org.apache.kafka.common.utils.Time

object Main extends CommandApp(
  name = "hunger-artist-examples",
  header = "Run an example program from the hunger-artist repetoire",
  main = {

    Opts.argument[String]("group-id")
      .map { groupId =>

        val networkClient =
          AsyncClient.networkClient(
            brokers = List(new InetSocketAddress("localhost", 9092)),
            config = AsyncClient.Config("test-program")
          )

        val client = new AsyncClient(networkClient, Time.SYSTEM)

        val group = GroupClient(client, groupId)

        val whatever = new GroupClient.Partitioned(client, new RoundRobinAssignor, Set("test-topic"), { stuff =>

          IO {
            println(stuff)
          }
        })

        val done = group.runGroup(whatever)

        done.unsafeRunSync()
      }
  }
)
