package storage

import "testing"

func TestIterationOptionsValidation(t *testing.T) {
	itOps := new(iterOpts)
	if _, present := itOps.KeyPrefix(); present {
		t.Errorf("Expected no key prefix to be set")
	}

	if _, present := itOps.StartKey(); present {
		t.Errorf("Expected no start key to be set")
	}

	expKeyPrefix := "pref"
	itOps.keyPrefix = []byte(expKeyPrefix)
	if kp, present := itOps.KeyPrefix(); !present || string(kp) != expKeyPrefix {
		t.Errorf("Expected key prefix to be set and equal to %s", expKeyPrefix)
	}

	if err := itOps.validate(); err != nil {
		t.Errorf("Expected no validation errors but got %v", err)
	}

	expStartKey := "start"
	itOps.startKey = []byte(expStartKey)
	if sk, present := itOps.StartKey(); !present || string(sk) != expStartKey {
		t.Errorf("Expected start key to be set and equal to %s", expStartKey)
	}

	if err := itOps.validate(); err == nil {
		t.Errorf("Expected validation errors but got none")
	} else {
		t.Log(err)
	}

	expStartKey = expKeyPrefix + expStartKey
	itOps.startKey = []byte(expStartKey)

	if err := itOps.validate(); err != nil {
		t.Errorf("Expected no validation error. But got error: %v", err)
	}
}
