package tukxdw

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ipthomas/tukdbint"
	"github.com/ipthomas/tukutil"

	"github.com/ipthomas/tukcnst"

	"github.com/ipthomas/tukdsub"
)

type DSUBSubscribeResponse struct {
	XMLName        xml.Name `xml:"Envelope"`
	Text           string   `xml:",chardata"`
	S              string   `xml:"s,attr"`
	A              string   `xml:"a,attr"`
	Xsi            string   `xml:"xsi,attr"`
	Wsnt           string   `xml:"wsnt,attr"`
	SchemaLocation string   `xml:"schemaLocation,attr"`
	Header         struct {
		Text   string `xml:",chardata"`
		Action string `xml:"Action"`
	} `xml:"Header"`
	Body struct {
		Text              string `xml:",chardata"`
		SubscribeResponse struct {
			Text                  string `xml:",chardata"`
			SubscriptionReference struct {
				Text    string `xml:",chardata"`
				Address string `xml:"Address"`
			} `xml:"SubscriptionReference"`
		} `xml:"SubscribeResponse"`
	} `xml:"Body"`
}
type DSUBSubscribe struct {
	BrokerUrl   string
	ConsumerUrl string
	Topic       string
	Expression  string
	Request     []byte
	BrokerRef   string
	UUID        string
}
type WorkflowDefinition struct {
	Ref                 string `json:"ref"`
	Name                string `json:"name"`
	Confidentialitycode string `json:"confidentialitycode"`
	CompleteByTime      string `json:"completebytime"`
	CompletionBehavior  []struct {
		Completion struct {
			Condition string `json:"condition"`
		} `json:"completion"`
	} `json:"completionBehavior"`
	Tasks []struct {
		ID                 string `json:"id"`
		Tasktype           string `json:"tasktype"`
		Name               string `json:"name"`
		Description        string `json:"description"`
		Owner              string `json:"owner"`
		ExpirationTime     string `json:"expirationtime"`
		StartByTime        string `json:"startbytime"`
		CompleteByTime     string `json:"completebytime"`
		IsSkipable         bool   `json:"isskipable"`
		CompletionBehavior []struct {
			Completion struct {
				Condition string `json:"condition"`
			} `json:"completion"`
		} `json:"completionBehavior"`
		Input []struct {
			Name        string `json:"name"`
			Contenttype string `json:"contenttype"`
			AccessType  string `json:"accesstype"`
		} `json:"input,omitempty"`
		Output []struct {
			Name        string `json:"name"`
			Contenttype string `json:"contenttype"`
			AccessType  string `json:"accesstype"`
		} `json:"output,omitempty"`
	} `json:"tasks"`
}
type XDWTransaction struct {
	Action           string
	Pathway          string
	NHS_ID           string
	Task_ID          int
	DSUB_BrokerURL   string
	DSUB_ConsumerURL string
	Request          []byte
	Response         []byte
	XDWDefinition    WorkflowDefinition
	XDWDocument      XDWWorkflowDocument
	XDWVersion       int
	XDWState         XDWState
}
type XDWState struct {
	Created                 string
	CompleteBy              string
	Status                  string
	IsPublished             bool
	IsOverdue               bool
	LatestWorkflowEventTime time.Time
	LatestTaskEventTime     time.Time
	WorkflowDuration        time.Duration
	PrettyWorkflowDuration  string
}

// XDW Workflow Document Structs

type XDWWorkflowDocument struct {
	XMLName                        xml.Name              `xml:"XDW.WorkflowDocument"`
	Hl7                            string                `xml:"hl7,attr"`
	WsHt                           string                `xml:"ws-ht,attr"`
	Xdw                            string                `xml:"xdw,attr"`
	Xsi                            string                `xml:"xsi,attr"`
	SchemaLocation                 string                `xml:"schemaLocation,attr"`
	ID                             ID                    `xml:"id"`
	EffectiveTime                  EffectiveTime         `xml:"effectiveTime"`
	ConfidentialityCode            ConfidentialityCode   `xml:"confidentialityCode"`
	Patient                        PatientID             `xml:"patient"`
	Author                         Author                `xml:"author"`
	WorkflowInstanceId             string                `xml:"workflowInstanceId"`
	WorkflowDocumentSequenceNumber string                `xml:"workflowDocumentSequenceNumber"`
	WorkflowStatus                 string                `xml:"workflowStatus"`
	WorkflowStatusHistory          WorkflowStatusHistory `xml:"workflowStatusHistory"`
	WorkflowDefinitionReference    string                `xml:"workflowDefinitionReference"`
	TaskList                       TaskList              `xml:"TaskList"`
}
type ConfidentialityCode struct {
	Code string `xml:"code,attr"`
}
type EffectiveTime struct {
	Value string `xml:"value,attr"`
}
type PatientID struct {
	ID ID `xml:"id"`
}
type Author struct {
	AssignedAuthor AssignedAuthor `xml:"assignedAuthor"`
}
type AssignedAuthor struct {
	ID             ID             `xml:"id"`
	AssignedPerson AssignedPerson `xml:"assignedPerson"`
}
type ID struct {
	Root                   string `xml:"root,attr"`
	Extension              string `xml:"extension,attr"`
	AssigningAuthorityName string `xml:"assigningAuthorityName,attr"`
}
type AssignedPerson struct {
	Name Name `xml:"name"`
}
type Name struct {
	Family string `xml:"family"`
	Prefix string `xml:"prefix"`
}
type WorkflowStatusHistory struct {
	DocumentEvent []DocumentEvent `xml:"documentEvent"`
}
type TaskList struct {
	XDWTask []XDWTask `xml:"XDWTask"`
}
type XDWTask struct {
	TaskData         TaskData         `xml:"taskData"`
	TaskEventHistory TaskEventHistory `xml:"taskEventHistory"`
}
type TaskData struct {
	TaskDetails TaskDetails `xml:"taskDetails"`
	Description string      `xml:"description"`
	Input       []Input     `xml:"input"`
	Output      []Output    `xml:"output"`
}
type TaskDetails struct {
	ID                    string `xml:"id"`
	TaskType              string `xml:"taskType"`
	Name                  string `xml:"name"`
	Status                string `xml:"status"`
	ActualOwner           string `xml:"actualOwner"`
	CreatedTime           string `xml:"createdTime"`
	CreatedBy             string `xml:"createdBy"`
	LastModifiedTime      string `xml:"lastModifiedTime"`
	RenderingMethodExists string `xml:"renderingMethodExists"`
}
type TaskEventHistory struct {
	TaskEvent []TaskEvent `xml:"taskEvent"`
}
type AttachmentInfo struct {
	Identifier      string `xml:"identifier"`
	Name            string `xml:"name"`
	AccessType      string `xml:"accessType"`
	ContentType     string `xml:"contentType"`
	ContentCategory string `xml:"contentCategory"`
	AttachedTime    string `xml:"attachedTime"`
	AttachedBy      string `xml:"attachedBy"`
	HomeCommunityId string `xml:"homeCommunityId"`
}
type Part struct {
	Name           string         `xml:"name,attr"`
	AttachmentInfo AttachmentInfo `xml:"attachmentInfo"`
}
type Output struct {
	Part Part `xml:"part"`
}
type Input struct {
	Part Part `xml:"part"`
}
type DocumentEvent struct {
	EventTime           string `xml:"eventTime"`
	EventType           string `xml:"eventType"`
	TaskEventIdentifier string `xml:"taskEventIdentifier"`
	Author              string `xml:"author"`
	PreviousStatus      string `xml:"previousStatus"`
	ActualStatus        string `xml:"actualStatus"`
}
type TaskEvent struct {
	ID         string `xml:"id"`
	EventTime  string `xml:"eventTime"`
	Identifier string `xml:"identifier"`
	EventType  string `xml:"eventType"`
	Status     string `xml:"status"`
}
type DocumentEvents []DocumentEvent

func (e DocumentEvents) Len() int {
	return len(e)
}
func (e DocumentEvents) Less(i, j int) bool {
	return e[i].EventTime > e[j].EventTime
}
func (e DocumentEvents) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}
func (i *XDWTransaction) NewXDWContentConsumer() error {
	var err error
	if err = i.setWorkflowState(); err != nil {
		log.Println(err.Error())
	}
	return err
}
func (i *XDWTransaction) RegisterWorkflowDefinition() error {
	i.XDWDefinition = WorkflowDefinition{}
	err := json.Unmarshal(i.Request, &i.XDWDefinition)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	i.Pathway = i.XDWDefinition.Ref
	err = i.registerWorkflowDefinition()
	if err != nil {
		log.Println(err.Error())
	}
	return err
}
func (i *XDWTransaction) registerWorkflowDefinition() error {
	var err error
	pwyExpressions := make(map[string]string)
	if err = i.persistXDWDefinition(); err == nil {
		log.Println("Parsing XDW Tasks for potential DSUB Broker Subscriptions")
		for _, task := range i.XDWDefinition.Tasks {
			for _, inp := range task.Input {
				log.Printf("Checking Input Task %s", inp.Name)
				if inp.AccessType == tukcnst.XDS_REGISTERED {
					pwyExpressions[inp.Name] = i.XDWDefinition.Ref
					log.Printf("Task %v %s task input %s included in potential DSUB Broker subscriptions", task.ID, task.Name, inp.Name)
				} else {
					log.Printf("Input Task %s does not require a dsub broker subscription", inp.Name)
				}
			}
			for _, out := range task.Output {
				log.Printf("Checking Output Task %s", out.Name)
				if out.AccessType == tukcnst.XDS_REGISTERED {
					pwyExpressions[out.Name] = i.XDWDefinition.Ref
					log.Printf("Task %v %s task output %s included in potential DSUB Broker subscriptions", task.ID, task.Name, out.Name)
				} else {
					log.Printf("Output Task %s does not require a dsub broker subscription", out.Name)
				}
			}
		}
	}
	log.Printf("Found %v potential DSUB Broker Subscriptions - %s", len(pwyExpressions), pwyExpressions)
	if len(pwyExpressions) > 0 {
		event := tukdsub.DSUBEvent{Action: tukcnst.CANCEL, Pathway: i.XDWDefinition.Ref}
		tukdsub.New_Transaction(&event)
		event.Action = tukcnst.CREATE
		event.BrokerURL = i.DSUB_BrokerURL
		event.ConsumerURL = i.DSUB_ConsumerURL
		for expression := range pwyExpressions {
			event.Expressions = append(event.Expressions, expression)
		}
		err = tukdsub.New_Transaction(&event)
	}
	return err
}
func (i *XDWTransaction) persistXDWDefinition() error {
	log.Println("Processing WF Def for Pathway : " + i.XDWDefinition.Ref)
	xdw := tukdbint.XDW{Name: i.XDWDefinition.Ref}
	xdws := tukdbint.XDWS{Action: tukcnst.DELETE}
	xdws.XDW = append(xdws.XDW, xdw)
	if err := tukdbint.NewDBEvent(&xdws); err != nil {
		log.Println(err.Error())
		return err
	}
	log.Printf("Deleted Existing XDW Definition for Pathway %s", i.XDWDefinition.Ref)

	xdwBytes, _ := json.Marshal(i.XDWDefinition)
	xdw = tukdbint.XDW{Name: i.XDWDefinition.Ref, IsXDSMeta: false, XDW: string(xdwBytes)}
	xdws = tukdbint.XDWS{Action: tukcnst.INSERT}
	xdws.XDW = append(xdws.XDW, xdw)
	if err := tukdbint.NewDBEvent(&xdws); err != nil {
		log.Println(err.Error())
		return err
	}
	log.Printf("Persisted New XDW Definition for Pathway %s", i.XDWDefinition.Ref)
	return nil
}
func (i *XDWTransaction) SetTaskLastModifiedTime() error {
	if i.XDWDocument.WorkflowStatus == "" || i.Task_ID < 1 {
		errstr := "invalid request xdwtransaction must have a valid workflowdocument and workflow task id set"
		log.Println(errstr)
		return errors.New(errstr)
	}
	for _, task := range i.XDWDocument.TaskList.XDWTask {
		if task.TaskData.TaskDetails.ID == tukutil.GetStringFromInt(i.Task_ID) {
			i.XDWState.LatestTaskEventTime, _ = time.Parse(time.RFC3339, task.TaskData.TaskDetails.LastModifiedTime)
		}
	}
	return nil
}
func (i *XDWTransaction) SetWorkflowDuration() {
	ws, err := time.Parse(time.RFC3339, i.XDWDocument.EffectiveTime.Value)
	if err != nil {
		fmt.Println(err)
	}
	we := time.Now()
	if i.XDWDocument.WorkflowStatus == tukcnst.COMPLETE {
		we = i.XDWState.LatestWorkflowEventTime
	}
	i.XDWState.WorkflowDuration = we.Sub(ws)
	log.Println("Duration - " + i.XDWState.WorkflowDuration.String())
	//25h15m32.877428s
	totmins := int(i.XDWState.WorkflowDuration.Minutes())
	var onehour = 60
	var oneday = onehour * 24
	if totmins < onehour {
		if totmins == 0 {
			i.XDWState.PrettyWorkflowDuration = "Less than a Minute"
		} else {
			if totmins == 1 {
				i.XDWState.PrettyWorkflowDuration = "1 Min"
			} else {
				i.XDWState.PrettyWorkflowDuration = tukutil.GetStringFromInt(totmins) + " Mins"
			}
		}
	} else {
		if totmins < oneday {
			hrs := totmins / onehour
			if hrs == 0 {
				i.XDWState.PrettyWorkflowDuration = "1 Hour"
			} else {
				i.XDWState.PrettyWorkflowDuration = tukutil.GetStringFromInt(hrs) + " Hours " + tukutil.GetStringFromInt(totmins-(hrs*onehour)) + " Mins"
			}

		} else {
			days := totmins / oneday
			if days == 0 {
				i.XDWState.PrettyWorkflowDuration = "1 Day"
			} else {
				hrs := totmins - (days * oneday)
				mins := totmins - (days * oneday) - (hrs * onehour)
				i.XDWState.PrettyWorkflowDuration = tukutil.GetStringFromInt(days) + " Days " + tukutil.GetStringFromInt(hrs) + " Hrs " + tukutil.GetStringFromInt(mins) + " Mins"
			}
		}
	}
}
func (i *XDWTransaction) SetLatestWorkflowEventTime() error {
	log.Printf("Setting Latest Workflow Event Time for Pathway %s NHS ID %s", i.Pathway, i.NHS_ID)
	lastevent, err := time.Parse(time.RFC3339, i.XDWDocument.EffectiveTime.Value)
	if err != nil {
		log.Println(err.Error())
	}
	for _, docevent := range i.XDWDocument.WorkflowStatusHistory.DocumentEvent {
		etime, err := time.Parse(time.RFC3339, docevent.EventTime)
		if err != nil {
			log.Println(err.Error())
		}
		if etime.After(lastevent) {
			log.Printf("Workflow Event Time %s is later than last workflow Event Time %s. Updated Latest Event Time", etime, lastevent)
			lastevent = etime
		}
	}
	i.XDWState.LatestWorkflowEventTime = lastevent
	log.Printf("Latest Event Time set to %s ", lastevent.String())
	return nil
}
func (i *XDWTransaction) setWorkflowState() error {
	log.Printf("Setting Workflow State for Pathway %s NHS ID %s Version %v", i.Pathway, i.NHS_ID, i.XDWVersion)
	wfs := tukdbint.Workflows{Action: tukcnst.SELECT}
	wf := tukdbint.Workflow{XDW_Key: strings.ToUpper(i.Pathway) + i.NHS_ID, Version: i.XDWVersion}
	wfs.Workflows = append(wfs.Workflows, wf)
	if err := tukdbint.NewDBEvent(&wfs); err != nil {
		log.Println(err.Error())
		return err
	}
	if wfs.Count == 1 {
		if err := json.Unmarshal([]byte(wfs.Workflows[1].XDW_Def), &i.XDWDefinition); err != nil {
			log.Println(err.Error())
			return err
		}
		if err := json.Unmarshal([]byte(wfs.Workflows[1].XDW_Doc), &i.XDWDocument); err != nil {
			log.Println(err.Error())
			return err
		}
		i.XDWState.Created = i.XDWDocument.EffectiveTime.Value
		i.XDWState.Status = i.XDWDocument.WorkflowStatus
		i.XDWState.IsPublished = wfs.Workflows[1].Published
		if i.XDWDefinition.CompleteByTime == "" {
			i.XDWState.CompleteBy = "Non Specified"
		}
		workflowStartTime := tukutil.GetTimeFromString(i.XDWState.Created)
		days := tukutil.GetIntFromString(strings.Split(strings.Split(i.XDWDefinition.CompleteByTime, "(")[1], ")")[0])
		i.XDWState.CompleteBy = strings.Split(tukutil.GetFutueDaysDate(workflowStartTime, days).String(), " +0")[0]
		i.SetLatestWorkflowEventTime()
		i.SetWorkflowDuration()
		if i.XDWDefinition.CompleteByTime == "" {
			i.XDWState.IsOverdue = false
		} else {
			completionDate := tukutil.GetFutueDaysDate(workflowStartTime, days)
			if i.XDWState.Status == tukcnst.COMPLETE {
				i.XDWState.IsOverdue = i.XDWState.LatestWorkflowEventTime.After(completionDate)
			} else {
				i.XDWState.IsOverdue = time.Now().After(completionDate)
			}
		}
	} else {
		log.Println("No Workflow Found")
	}
	return nil
}
