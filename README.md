# Distributed Quota

Distributed quota is a counter mechanism that makes use of CRDTs to enable a quota across multiple processes.
It is designed to to work with the Tyk Cluster Framework as it requires a mechanism to broadcast it's status.

## Example usage

```
    topic := "dq.tests.quotas"
    cs := "redis://localhost:6379"

    // Create clients to handle comms
    c1, _ := client.NewClient(cs, encoding.JSON)
    c2, _ := client.NewClient(cs, encoding.JSON)
    store := simpleStorage{data: make(map[string]Quota)}

    // Create the DQ
    // See the tests for a sample flusher, this can be a simple function
    d1 := NewDQ(store.Flusher, eh, "node1")
    d1.BroadcastWith(c1, time.Millisecond*100, topic)

    // We need a leader to flush data to disk
    d1.SetLeader(true)

    d2 := NewDQ(store.Flusher, eh, "node2")
    d2.BroadcastWith(c2, time.Millisecond*100, topic)

    // Start everything up
    if err := d1.Start(); err != nil {
        panic(err)
    }

    if err := d2.Start(); err != nil {
        panic(err)
    }

    // Try a quota
    d1.InitQuota(10, 0, "test-quota-2")
    d2.InitQuota(10, 0, "test-quota-2")

    // Increment the quota by 2
    qs1 := d1.IncrBy("test-quota-2", 2)

    // lets keep incrementing on the second instance
    for i := 0; i <= 10; i++ {
        qst := d2.IncrBy("test-quota-2", 1)

        if qst == quota_violated {
            break
        }


        time.Sleep(time.Millisecond * 50)
    }
``