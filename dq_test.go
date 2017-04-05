package dq

import (
	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/TykTechnologies/tyk-cluster-framework/encoding"
	"log"
	"os"
	"testing"
	"time"
)

type simpleStorage struct {
	Data map[string]Quota
}

func (s *simpleStorage) Flusher(d map[string]*Quota) error {
	for k, v := range d {
		s.Data[k] = *v
	}

	return nil

}

func eh(err error) {
	log.Fatal(err)
}

func TestDQ(t *testing.T) {
	nid1 := "node1"
	nid2 := "node2"
	topic := "dq.tests.quotas"

	redisServer := os.Getenv("TCF_TEST_REDIS")
	if redisServer == "" {
		redisServer = "localhost:6379"
	}
	cs := "redis://" + redisServer

	c1, _ := client.NewClient(cs, encoding.JSON)
	c2, _ := client.NewClient(cs, encoding.JSON)
	store := simpleStorage{Data: make(map[string]Quota)}

	d1 := NewDQ(store.Flusher, eh, nid1)
	d1.BroadcastWith(c1, time.Millisecond*100, topic)
	d1.SetLeader(true)

	// Far too high, but ok for tests
	d1.FlushInterval = time.Millisecond * 50

	d2 := NewDQ(store.Flusher, eh, nid2)
	d2.BroadcastWith(c2, time.Millisecond*100, topic)

	if err := d1.Start(); err != nil {
		t.Fatal(err)
	}

	if err := d2.Start(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 2)

	t.Run("Test Basic DQ", func(t *testing.T) {
		d1.InitQuota(5, 0, "test-quota-1")

		qs1 := d1.IncrBy("test-quota-1", 5)
		if qs1 != quota_violated {
			t.Fatalf("Quota should be violated, got: %v\n", qs1)
		}
	})

	t.Run("Test Distributed and Initialised DQ", func(t *testing.T) {
		d1.InitQuota(10, 0, "test-quota-2")
		d2.InitQuota(10, 0, "test-quota-2")

		qs1 := d1.IncrBy("test-quota-2", 2)
		if qs1 == quota_violated {
			t.Fatalf("Quota should not be violated, got: %v\n", qs1)
		}

		// lets keep incrementing on dq2
		for i := 0; i <= 10; i++ {
			qst := d2.IncrBy("test-quota-2", 1)
			if i >= 8 {
				if qst != quota_violated {
					t.Fatal("Quota should be violated!")
				}
			}
			if qst == quota_violated {
				break
			}
			time.Sleep(time.Millisecond * 50)
		}
	})

	t.Run("Test Distributed and Initialised DQ Two-way", func(t *testing.T) {
		d1.InitQuota(10, 0, "test-quota-3")
		d2.InitQuota(10, 0, "test-quota-3")

		qs1 := d1.IncrBy("test-quota-3", 2)
		if qs1 == quota_violated {
			t.Fatalf("Quota should not be violated, got: %v\n", qs1)
		}

		// lets keep incrementing on dq2
		for i := 0; i <= 5; i++ {
			qst := d2.IncrBy("test-quota-3", 1)
			if qst == quota_violated {
				t.Fatal("Quota should not be vilated at all")
			}
			time.Sleep(time.Millisecond * 50)
		}

		// Quota is now 7
		for i := 0; i <= 5; i++ {
			qst := d1.IncrBy("test-quota-3", 1)
			if i >= 3 {
				if qst != quota_violated {
					t.Fatal("Quota should be violated!")
				}
			}

			if qst == quota_violated {
				break
			}

			time.Sleep(time.Millisecond * 50)
		}

	})

	t.Run("Test Distributed and Initialised DQ Two-way with delete", func(t *testing.T) {
		d1.InitQuota(10, 0, "test-quota-4")
		d2.InitQuota(10, 0, "test-quota-4")

		qs1 := d1.IncrBy("test-quota-4", 2)
		if qs1 == quota_violated {
			t.Fatalf("Quota should not be violated, got: %v\n", qs1)
		}

		// lets keep incrementing on dq2
		for i := 0; i <= 5; i++ {
			qst := d2.IncrBy("test-quota-4", 1)
			if qst == quota_violated {
				t.Fatal("Quota should not be vilated at all")
			}
			time.Sleep(time.Millisecond * 50)
		}

		// Quota is now 7
		d2.Delete("test-quota-4")
		// Quota should no just be 0
		for i := 0; i <= 5; i++ {
			qst := d1.IncrBy("test-quota-4", 1)
			if i >= 3 {
				if qst != quota_not_found {
					t.Fatalf("Quota should be not found, got different status though: %v\n!", qst)
				}
			}

			if qst == quota_violated {
				break
			}

			time.Sleep(time.Millisecond * 50)
		}

	})

	t.Run("Test Distributed and Initialised DQ Two-way with delete and reinit", func(t *testing.T) {
		d1.InitQuota(10, 0, "test-quota-5")
		d2.InitQuota(10, 0, "test-quota-5")

		qs1 := d1.IncrBy("test-quota-5", 2)
		if qs1 == quota_violated {
			t.Fatalf("Quota should not be violated, got: %v\n", qs1)
		}

		// lets keep incrementing on dq2
		for i := 0; i <= 5; i++ {
			qst := d2.IncrBy("test-quota-5", 1)
			if qst == quota_violated {
				t.Fatal("Quota should not be vilated at all")
			}
			time.Sleep(time.Millisecond * 50)
		}

		// Quota is now 7
		d2.Delete("test-quota-5")
		d2.InitQuota(5, 0, "test-quota-5")
		d2.IncrBy("test-quota4-5", 2)
		// Quota should now be 2
		for i := 0; i <= 5; i++ {
			qst := d1.IncrBy("test-quota-5", 1)
			if i >= 3 {
				if qst != quota_violated {
					t.Fatalf("Quota should be be violated, got: %v\n!", qst)
				}
			}

			if qst == quota_violated {
				break
			}

			time.Sleep(time.Millisecond * 50)
		}

	})

	// Lets check the flusher works
	time.Sleep(1 * time.Second)

	expectedKeys := map[string]bool{
		"test-quota-1": true,
		"test-quota-2": true,
		"test-quota-3": true,
		"test-quota-4": true,
		"test-quota-5": true,
	}

	for k, _ := range expectedKeys {
		c, f := store.Data[k]
		if !f {
			t.Fatalf("Store couldn't find key: %v\n", k)
		}

		if c.Counter.Count() == 0 {
			t.Fatalf("Counter for '%v' is 0, should be higher", k)
		}
	}
}
