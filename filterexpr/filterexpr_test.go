package filterexpr

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInvalid(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile("a:")
	assert.EqualError(err, `1:3 (2): no match found, expected: "-", "/", "0", "\"", "false", "null", "true", [ \t\r\n] or [1-9]`)
	assert.Nil(filter)
}

func TestSimpleKeyVal(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a:"b"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "b"}))
	assert.False(filter.Test(map[string]interface{}{"a": "a"}))
}

func TestSimpleKeyValWithSpaces(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a: "b"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "b"}))
	assert.False(filter.Test(map[string]interface{}{"a": "a"}))
	filter, err = Compile(`a : "b"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "b"}))
	assert.False(filter.Test(map[string]interface{}{"a": "a"}))
}

func TestSimpleKeyValEscaped(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a:"\"hello\""`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": `"hello"`}))
}

func TestSimpleKeyValNum(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a:123`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "123"}))
	assert.True(filter.Test(map[string]interface{}{"a": 123}))
	assert.False(filter.Test(map[string]interface{}{"a": "456"}))
}

func TestSimpleKeyValBool(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a:true`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": true}))
	assert.False(filter.Test(map[string]interface{}{"a": false}))
	filter, err = Compile(`a:false`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": false}))
	assert.False(filter.Test(map[string]interface{}{"a": true}))
}

func TestSimpleKeyValWithDots(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a.b:"true"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{
		"a": map[string]interface{}{
			"b": true,
		},
	}))
	assert.True(filter.Test(map[string]interface{}{
		"a": map[string]interface{}{
			"b": "true",
		},
	}))
	filter, err = Compile(`a.b:true`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{
		"a": map[string]interface{}{
			"b": true,
		},
	}))
	assert.True(filter.Test(map[string]interface{}{
		"a": map[string]interface{}{
			"b": "true",
		},
	}))
}

func TestWithOR(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a:"b" OR b:"a"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "b"}))
	assert.True(filter.Test(map[string]interface{}{"a": "a", "b": "a"}))
}

func TestWithAND(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a:"b" AND b:"a"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "b", "b": "a"}))
	assert.False(filter.Test(map[string]interface{}{"a": "b", "b": "c"}))
}

func TestWithANDGroup(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`(a:"a" OR b:"b") AND c:"c"`)
	assert.NoError(err)
	assert.False(filter.Test(map[string]interface{}{"a": "a", "b": "b"}))
	assert.True(filter.Test(map[string]interface{}{"a": "a", "b": "a", "c": "c"}))
	assert.True(filter.Test(map[string]interface{}{"a": "b", "b": "b", "c": "c"}))
}

func TestWithORGroup(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`(a:"a" OR b:"b") OR (c:"c" d:true)`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "a", "b": "b", "c": "c", "d": true}))
	assert.True(filter.Test(map[string]interface{}{"a": "b", "b": "b", "c": "d", "d": false}))
}

func TestRegexp(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`a:/a/`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "a"}))
	assert.False(filter.Test(map[string]interface{}{"a": "ABC"}))
	filter, err = Compile(`a:/(?i)a/`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "ABC"}))
	filter, err = Compile(`a:/^a$/`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "a"}))
	assert.False(filter.Test(map[string]interface{}{"a": "b"}))
	filter, err = Compile(`a:/\d+/`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": 123}))
	filter, err = Compile(`a:/\d{1,3}/`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": 123}))
	filter, err = Compile(`a:/^admin\./`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "admin.Agent"}))
	assert.True(filter.Test(map[string]interface{}{"a": "admin.AgentLastUpdate"}))
	assert.False(filter.Test(map[string]interface{}{"a": "adminAgentLastUpdate"}))
	filter, err = Compile(`a:/Hi \d+/`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "Hi 123"}))
}

func TestUnderscoreInKey(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`user_id:"a"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"user_id": "a"}))
}

func TestDashInKey(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`user-id:"a"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"user-id": "a"}))
}

func TestABigAsExpression(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`(model:"activityfeed.Feed" AND user_id:"40bfb0d341249a58") OR model:"admin.Integration" OR model:"admin.RepoList" OR model:"admin.ProjectList" OR model:"admin.CalendarList" OR model:"admin.Agent" OR model:"admin.Customer" OR model:"admin.CustomerInformation" OR model:"admin.CustomerSubscription" OR model:"admin.User" OR model:"admin.UserMapping" OR model:"admin.Profile" OR model:"customer.Team" OR model:"pipeline.work.Issue" OR model:"pipeline.work.Sprint" OR model:"pipeline.work.Project" OR model:"pipeline.sourcecode.PullRequest" OR model:"pipeline.sourcecode.Repo" OR model:"admin.IntegrationUser" OR model:"pipeline.work.Retro" OR model:"pipeline.work.RetroNoteGrouping" OR model:"pipeline.work.RetroUserRating" OR model:"pipeline.work.RetroNote" OR model:"pipeline.work.RetroNoteVote" OR model:"pipeline.work.RetroNoteGroupingVote"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"model": "activityfeed.Feed", "user_id": "40bfb0d341249a58"}))
}

func TestStringify(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`user-id:"a"`)
	assert.NoError(err)
	assert.Equal("ExpressionList[[ExpressionGroup[[Expression[Node[user-id=a],,<nil>]]]]]", filter.String())
}

func TestMultipleAndClausesWithJoinedOr(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`(model:"A" AND user_id:"1") OR (model:"B" AND user_id:"2") OR (model:"C" AND user_id:"3") OR (model:"D" AND user_id:"4") OR (model:"E" AND user_id:"5")`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"model": "A", "user_id": "1"}))
	assert.True(filter.Test(map[string]interface{}{"model": "B", "user_id": "2"}))
	assert.True(filter.Test(map[string]interface{}{"model": "C", "user_id": "3"}))
	assert.True(filter.Test(map[string]interface{}{"model": "D", "user_id": "4"}))
	assert.True(filter.Test(map[string]interface{}{"model": "E", "user_id": "5"}))
	assert.False(filter.Test(map[string]interface{}{"model": "E", "user_id": "6"}))
}

func TestMultipleAndClausesWithJoinedAnd(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`(a:"A" AND b:"B") AND (c:"C" AND d:"D")`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "A", "b": "B", "c": "C", "d": "D"}))
	assert.False(filter.Test(map[string]interface{}{"a": "A", "b": "B", "c": "C", "d": "d"}))
}

func TestMultipleAndClausesWithJoinedMixedAndOr1(t *testing.T) {
	assert := assert.New(t)
	// filter, err := Compile(`(model:"activityfeed.Bookmark" AND user_id:"ecf8fbd624bb9c39") OR (model:"activityfeed.Feed" AND user_id:"ecf8fbd624bb9c39") OR model:"admin.Integration" OR model:"admin.RepoList" OR model:"admin.ProjectList" OR model:"admin.CalendarList" OR model:"admin.Agent" OR model:"admin.Customer" OR model:"admin.CustomerInformation" OR model:"admin.CustomerSubscription" OR model:"admin.User" OR model:"admin.UserMapping" OR model:"admin.Profile" OR model:"registry.Publisher" OR model:"registry.Integration" OR model:"customer.Team" OR model:"pipeline.work.Issue" OR model:"pipeline.work.Sprint" OR model:"pipeline.work.Project" OR model:"pipeline.sourcecode.PullRequest" OR model:"pipeline.sourcecode.PullRequestReview" OR model:"pipeline.sourcecode.Repo" OR model:"admin.IntegrationUser" OR model:"pipeline.work.Retro" OR model:"pipeline.work.RetroNoteGrouping" OR model:"pipeline.work.RetroUserRating" OR model:"pipeline.work.RetroNote" OR model:"pipeline.work.RetroNoteVote" OR model:"pipeline.work.RetroNoteGroupingVote" OR model:"pipeline.work.RetroAction" OR model:"pipeline.work.RetroTalkingPoint" OR model:"datascience.SprintHealth" OR model:"datascience.IssueForecast" OR model:"admin.TeamSetting" OR model:"pipeline.work.Plan" OR model:"pipeline.work.PlanUser" OR model:"pipeline.work.PlannedIssue" OR model:"agent.Enrollment" OR model:"agent.IntegrationInstance" OR model:"registry.Release"`)
	filter, err := Compile(`(a:"A" AND b:"B") OR (c:"C" AND d:"D") AND (e:"e")`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"c": "C", "d": "D", "e": "e"}))
	assert.True(filter.Test(map[string]interface{}{"a": "A", "b": "B", "e": "e"}))
}

func TestGOLD117(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`(model:"activityfeed.Bookmark" AND user_id:"ecf8fbd624bb9c39") OR (model:"activityfeed.Feed" AND user_id:"ecf8fbd624bb9c39") OR model:"admin.Integration" OR model:"admin.RepoList" OR model:"admin.ProjectList" OR model:"admin.CalendarList" OR model:"admin.Agent" OR model:"admin.Customer" OR model:"admin.CustomerInformation" OR model:"admin.CustomerSubscription" OR model:"admin.User" OR model:"admin.UserMapping" OR model:"admin.Profile" OR model:"registry.Publisher" OR model:"registry.Integration" OR model:"customer.Team" OR model:"pipeline.work.Issue" OR model:"pipeline.work.Sprint" OR model:"pipeline.work.Project" OR model:"pipeline.sourcecode.PullRequest" OR model:"pipeline.sourcecode.PullRequestReview" OR model:"pipeline.sourcecode.Repo" OR model:"admin.IntegrationUser" OR model:"pipeline.work.Retro" OR model:"pipeline.work.RetroNoteGrouping" OR model:"pipeline.work.RetroUserRating" OR model:"pipeline.work.RetroNote" OR model:"pipeline.work.RetroNoteVote" OR model:"pipeline.work.RetroNoteGroupingVote" OR model:"pipeline.work.RetroAction" OR model:"pipeline.work.RetroTalkingPoint" OR model:"datascience.SprintHealth" OR model:"datascience.IssueForecast" OR model:"admin.TeamSetting" OR model:"pipeline.work.Plan" OR model:"pipeline.work.PlanUser" OR model:"pipeline.work.PlannedIssue" OR model:"agent.Enrollment" OR model:"agent.IntegrationInstance" OR model:"registry.Release"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"model": "activityfeed.Bookmark", "user_id": "ecf8fbd624bb9c39"}))
	assert.True(filter.Test(map[string]interface{}{"model": "activityfeed.Feed", "user_id": "ecf8fbd624bb9c39"}))
	assert.True(filter.Test(map[string]interface{}{"model": "admin.Integration"}))
}

func TestNullCheck(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`c:null`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "A", "b": "B", "c": nil}))
	assert.False(filter.Test(map[string]interface{}{"a": "A", "b": "B", "c": "C"}))
	assert.True(filter.Test(map[string]interface{}{"a": "A", "b": "B"}))
}

func TestArrayEqualsMatch(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`c:"foo"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "A", "b": "B", "c": []string{"foo", "bar"}}))
	filter, err = Compile(`c:"bar"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "A", "b": "B", "c": []string{"foo", "bar"}}))
	assert.False(filter.Test(map[string]interface{}{"a": "A", "b": "B", "c": []string{"gummy", "bear"}}))
	filter, err = Compile(`c:/^ba/`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "A", "b": "B", "c": []string{"foo", "bar"}}))
	filter, err = Compile(`c:"bar"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"a": "A", "b": "B", "c": []interface{}{"foo", "bar"}}))
}

func TestJSON(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`c.foo:"bar"`)
	assert.NoError(err)
	assert.True(filter.Test(map[string]interface{}{"c": `{"foo":"bar"}`}))
}

func TestEmbeddedJSON(t *testing.T) {
	assert := assert.New(t)
	filter, err := Compile(`data.attendee_ids:"ff1f0458cfe3e85d"`)
	assert.NoError(err)
	dbchange := `{
		"action": "UPSERT",
		"change_date": {
		  "epoch": 1600183776694,
		  "offset": 0,
		  "rfc3339": "2020-09-15T15:29:36.694+00:00"
		},
		"customer_id": "a8a78d9c16839b97",
		"data": "{\"_id\":\"088700e68ca935c2\",\"active\":false,\"attendee_ids\":[\"ff1f0458cfe3e85d\",\"579dc0e39f8f8bf8\"],\"attendee_ref_id\":\"\",\"busy\":false,\"calendar_id\":\"\",\"created_ts\":1600183776664,\"customer_id\":\"a8a78d9c16839b97\",\"description\":\"\",\"end_date\":{\"epoch\":1600182000000,\"offset\":-300,\"rfc3339\":\"2020-09-15T10:00:00-05:00\"},\"integration_instance_id\":\"fa191e168d0cdde8\",\"location\":{\"details\":\"\",\"name\":\"\",\"url\":\"https://meet.google.com/mia-cwgr-eih\"},\"name\":\"Gold Miners Standup\",\"owner\":{\"avatar_url\":\"https://lh3.googleusercontent.com/a-/AOh14GgENnGHM7EV8a9hMKeg8Up4zzD7em_0Abi-177x\",\"id\":\"b3810254256536ef\",\"name\":\"Robin Diddams\",\"nickname\":\"Robin\",\"profile_id\":\"79f9ee0efd869eb5\",\"ref_id\":\"579dc0e39f8f8bf8\",\"ref_type\":\"gcal\",\"team_id\":\"06fc70d80fdd028f\",\"type\":\"TRACKABLE\",\"url\":null},\"owner_ref_id\":\"579dc0e39f8f8bf8\",\"participants\":[],\"ref_id\":\"3kaqtn5lkri8iir1nba94mqt17_20200915T143000Z\",\"ref_type\":\"gcal\",\"start_date\":{\"epoch\":1600180200000,\"offset\":-300,\"rfc3339\":\"2020-09-15T09:30:00-05:00\"},\"status\":\"CONFIRMED\",\"updated_ts\":1600183776664,\"url\":\"https://www.google.com/calendar/event?eid=M2thcXRuNWxrcmk4aWlyMW5iYTk0bXF0MTdfMjAyMDA5MTVUMTQzMDAwWiByZGlkZGFtc0BwaW5wb2ludC5jb20\"}",
		"hashcode": "06556540c8ccaa4a",
		"id": "b20a52408f2df27d",
		"integration_instance_id": null,
		"model": "pipeline.calendar.Event",
		"ref_id": "088700e68ca935c2",
		"ref_type": ""
	 }`
	kv := make(map[string]interface{})
	json.Unmarshal([]byte(dbchange), &kv)
	assert.True(filter.Test(kv))
	filter, err = Compile(`data.attendee_ids:"ff1f0458cfe3e8ee"`)
	assert.NoError(err)
	assert.False(filter.Test(kv))
	filter, err = Compile(`data.attendee_ids:"ff1f0458cfe3e8ee" OR data.attendee_ids:"579dc0e39f8f8bf8"`)
	assert.NoError(err)
	assert.True(filter.Test(kv))
}
