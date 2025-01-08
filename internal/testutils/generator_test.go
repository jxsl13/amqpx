package testutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExchangeQueueAssertNextSubMsgOK(t *testing.T) {
	eqg := NewExchangeQueueGenerator(FuncName())

	eq := eqg()

	publishSeries := []string{}
	for i := 0; i < 10; i++ {
		publishSeries = append(publishSeries, eq.NextPubMsg())
	}

	for _, pubMsg := range publishSeries {
		eq.AssertNextSubMsg(t, pubMsg)

		// test that we can also have duplicate messages but only consecutive ones
		eq.AssertNextSubMsg(t, pubMsg)
	}
}

func TestExchangeQueueAssertNextSubMsgNotOK(t *testing.T) {
	eqg := NewExchangeQueueGenerator(FuncName())

	eq := eqg()

	eq.assertEqualFunc = func(t assert.TestingT, expected, actual any, msgAndArgs ...any) bool {
		// do not fail the test in case that the messages are not equal
		return assert.ObjectsAreEqual(expected, actual)
	}

	publishSeries := []string{}
	for i := 0; i < 10; i++ {
		msg := eq.NextPubMsg()
		if i%2 == 0 {
			// only add every second message to list
			publishSeries = append(publishSeries, msg)
		}
	}

	allAsserted := true
	for _, pubMsg := range publishSeries {
		matchesNextExpected := eq.AssertNextSubMsg(t, pubMsg)
		allAsserted = allAsserted && matchesNextExpected
	}

	require.False(t, allAsserted, "all messages should not be a consecutive series of messages")
}

func TestExchangeQueueAssertNextSubMsgNotOK_2(t *testing.T) {
	eqg := NewExchangeQueueGenerator(FuncName())

	eq := eqg()

	eq.assertEqualFunc = func(t assert.TestingT, expected, actual any, msgAndArgs ...any) bool {
		// do not fail the test in case that the messages are not equal
		return assert.ObjectsAreEqual(expected, actual)
	}

	publishSeries := []string{}
	for i := 0; i < 10; i++ {
		msg := eq.NextPubMsg()
		if i%2 == 1 { // change to 1
			// only add every second message to list
			publishSeries = append(publishSeries, msg)
		}
	}

	allAsserted := true
	for _, pubMsg := range publishSeries {
		matchesNextExpected := eq.AssertNextSubMsg(t, pubMsg)
		allAsserted = allAsserted && matchesNextExpected
	}

	require.False(t, allAsserted, "all messages should not be a consecutive series of messages")
}
