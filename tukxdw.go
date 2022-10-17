package tukxdw

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"log"
	"strconv"
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
	User             string
	Org              string
	Role             string
	Pathway          string
	NHS_ID           string
	Task_ID          int
	DSUB_BrokerURL   string
	DSUB_ConsumerURL string
	Request          []byte
	Response         []byte
	XDWDefinition    WorkflowDefinition
	XDSDocumentMeta  XDSDocumentMeta
	XDWDocument      XDWWorkflowDocument
	XDWVersion       int
	XDWState         XDWState
	XDWEvents        tukdbint.Events
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
type XDSDocumentMeta struct {
	ID                    string `json:"id"`
	Repositoryuniqueid    string `json:"repositoryuniqueid"`
	Registryoid           string `json:"registryoid"`
	Languagecode          string `json:"languagecode"`
	Docname               string `json:"docname"`
	Docdesc               string `json:"docdesc"`
	DocID                 string `json:"docid"`
	Authorinstitution     string `json:"authorinstitution"`
	Authorperson          string `json:"authorperson"`
	Classcode             string `json:"classcode"`
	Classcodescheme       string `json:"classcodescheme"`
	Classcodevalue        string `json:"classcodevalue"`
	Typecode              string `json:"typecode"`
	Typecodescheme        string `json:"typecodescheme"`
	Typecodevalue         string `json:"typecodevalue"`
	Practicesettingcode   string `json:"practicesettingcode"`
	Practicesettingscheme string `json:"practicesettingscheme"`
	Practicesettingvalue  string `json:"practicesettingvalue"`
	Confcode              string `json:"confcode"`
	Confcodescheme        string `json:"confcodescheme"`
	Confcodevalue         string `json:"confcodevalue"`
	Facilitycode          string `json:"facilitycode"`
	Facilitycodescheme    string `json:"facilitycodescheme"`
	Facilitycodevalue     string `json:"facilitycodevalue"`
	Formatcode            string `json:"formatcode"`
	Formatcodescheme      string `json:"formatcodescheme"`
	Formatcodevalue       string `json:"formatcodevalue"`
	Mimetype              string `json:"mimetype"`
	Objecttype            string `json:"objecttype"`
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
func (i *XDWTransaction) XDWContentCreator() error {

	return nil
}
func (i *XDWTransaction) NewXDWContentConsumer() error {
	var err error
	if err = i.setWorkflowState(); err != nil {
		log.Println(err.Error())
	}
	return err
}
func (i *XDWTransaction) RegisterWorkflowDefinition(ismeta bool) error {
	var err error
	if i.Pathway == "" {
		return errors.New("pathway is not set")
	}
	if i.Request == nil || string(i.Request) == "" {
		return errors.New("request bytes is not set")
	}
	if !ismeta {
		err = i.registerWorkflowDefinition()
		if err != nil {
			log.Println(err.Error())
		}
		return err
	} else {
		err = i.persistXDSMeta()
		if err != nil {
			log.Println(err.Error())
			return err
		}
	}
	return err
}
func (i *XDWTransaction) registerWorkflowDefinition() error {
	var err error
	err = json.Unmarshal(i.Request, &i.XDWDefinition)
	if err != nil {
		log.Println(err.Error())
		return err
	}
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
func (i *XDWTransaction) persistXDSMeta() error {
	log.Printf("Persisting XDS Meta for Pathway %s", i.Pathway)
	xdw := tukdbint.XDW{Name: i.Pathway + "_meta", IsXDSMeta: true}
	xdws := tukdbint.XDWS{Action: tukcnst.DELETE}
	xdws.XDW = append(xdws.XDW, xdw)
	if err := tukdbint.NewDBEvent(&xdws); err != nil {
		log.Println(err.Error())
		return err
	}
	log.Printf("Deleted Existing XDS Meta for Pathway %s", i.Pathway)

	xdw = tukdbint.XDW{Name: i.Pathway + "_meta", IsXDSMeta: true, XDW: string(i.Request)}
	xdws = tukdbint.XDWS{Action: tukcnst.INSERT}
	xdws.XDW = append(xdws.XDW, xdw)
	if err := tukdbint.NewDBEvent(&xdws); err != nil {
		log.Println(err.Error())
		return err
	}
	log.Printf("Persisted XDS meta for Pathway %s", i.Pathway)
	return nil
}
func (i *XDWTransaction) persistXDWDefinition() error {
	log.Println("Deleting WF Def for Pathway : " + i.Pathway)
	xdw := tukdbint.XDW{Name: i.Pathway, IsXDSMeta: false}
	xdws := tukdbint.XDWS{Action: tukcnst.DELETE}
	xdws.XDW = append(xdws.XDW, xdw)
	if err := tukdbint.NewDBEvent(&xdws); err != nil {
		log.Println(err.Error())
		return err
	}
	log.Printf("Deleted Existing XDW Definition for Pathway %s", i.Pathway)

	xdw = tukdbint.XDW{Name: i.XDWDefinition.Ref, IsXDSMeta: false, XDW: string(i.Request)}
	xdws = tukdbint.XDWS{Action: tukcnst.INSERT}
	xdws.XDW = append(xdws.XDW, xdw)
	if err := tukdbint.NewDBEvent(&xdws); err != nil {
		log.Println(err.Error())
		return err
	}
	log.Printf("Persisted New XDW Definition for Pathway %s", i.Pathway)
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
func (i *XDWTransaction) NewContentCreator() error {
	log.Printf("Creating New Workflow for %s", i.Pathway+i.NHS_ID)
	log.Printf("Obtaining XDS Meta for Pathway %s", i.Pathway)
	xdw := tukdbint.XDW{Name: i.Pathway + "_meta", IsXDSMeta: true}
	xdws := tukdbint.XDWS{Action: tukcnst.SELECT}
	xdws.XDW = append(xdws.XDW, xdw)
	if err := tukdbint.NewDBEvent(&xdws); err != nil {
		log.Println(err.Error())
		return err
	}
	if xdws.Count != 1 {
		return errors.New("no xds meta data found for pathway " + i.Pathway)
	}
	log.Printf("Loaded XDS Meta for Pathway %s", i.Pathway)
	if err := json.Unmarshal([]byte(xdws.XDW[1].XDW), &i.XDSDocumentMeta); err != nil {
		log.Println(err.Error())
		return err
	}
	xdw = tukdbint.XDW{Name: i.Pathway, IsXDSMeta: false}
	xdws = tukdbint.XDWS{Action: tukcnst.SELECT}
	xdws.XDW = append(xdws.XDW, xdw)
	if err := tukdbint.NewDBEvent(&xdws); err != nil {
		log.Println(err.Error())
		return err
	}
	if xdws.Count != 1 {
		return errors.New("no xdw def found for pathway " + i.Pathway)
	}
	log.Printf("Loaded XDW definition found for Pathway %s", i.Pathway)
	if err := json.Unmarshal([]byte(xdws.XDW[1].XDW), &i.XDWDefinition); err != nil {
		log.Println(err.Error())
		return err
	}
	log.Printf("Deprecating any current %s Workflow", i.Pathway+i.NHS_ID)
	wfs := tukdbint.Workflows{Action: tukcnst.DEPRECATE}
	wf := tukdbint.Workflow{XDW_Key: i.Pathway + i.NHS_ID}
	wfs.Workflows = append(wfs.Workflows, wf)
	if err := tukdbint.NewDBEvent(&wfs); err != nil {
		log.Println(err.Error())
	}
	var authoid = getLocalId(i.Org)
	var patoid = tukcnst.NHS_OID_DEFAULT
	var wfid = tukutil.Newid()
	var effectiveTime = time.Now().Format(time.RFC3339)
	i.XDWDocument.Xdw = tukcnst.XDWNameSpace
	i.XDWDocument.Hl7 = tukcnst.HL7NameSpace
	i.XDWDocument.WsHt = tukcnst.WHTNameSpace
	i.XDWDocument.Xsi = tukcnst.XMLNS_XSI
	i.XDWDocument.XMLName.Local = tukcnst.XDWNameLocal
	i.XDWDocument.SchemaLocation = tukcnst.WorkflowDocumentSchemaLocation
	i.XDWDocument.ID.Root = strings.ReplaceAll(tukcnst.WorkflowInstanceId, "^", "")
	i.XDWDocument.ID.Extension = wfid
	i.XDWDocument.ID.AssigningAuthorityName = "ICS"
	i.XDWDocument.EffectiveTime.Value = effectiveTime
	i.XDWDocument.ConfidentialityCode.Code = i.XDWDefinition.Confidentialitycode
	i.XDWDocument.Patient.ID.Root = patoid
	i.XDWDocument.Patient.ID.Extension = i.NHS_ID
	i.XDWDocument.Patient.ID.AssigningAuthorityName = "NHS"
	i.XDWDocument.Author.AssignedAuthor.ID.Root = authoid
	i.XDWDocument.Author.AssignedAuthor.ID.Extension = strings.ToUpper(i.Org)
	i.XDWDocument.Author.AssignedAuthor.ID.AssigningAuthorityName = getMappedId(i.Org)
	i.XDWDocument.Author.AssignedAuthor.AssignedPerson.Name.Family = i.User
	i.XDWDocument.Author.AssignedAuthor.AssignedPerson.Name.Prefix = i.Role
	i.XDWDocument.WorkflowInstanceId = wfid + tukcnst.WorkflowInstanceId
	i.XDWDocument.WorkflowDocumentSequenceNumber = "1"
	i.XDWDocument.WorkflowStatus = "READY"
	i.XDWDocument.WorkflowDefinitionReference = strings.ToUpper(i.Pathway)
	tevidstr := strconv.Itoa(int(i.newEventID()))
	for _, t := range i.XDWDefinition.Tasks {
		log.Println("Creating Task " + t.ID)
		task := XDWTask{}
		task.TaskData.TaskDetails.ID = t.ID
		task.TaskData.TaskDetails.TaskType = t.Tasktype
		task.TaskData.TaskDetails.Name = t.Name
		task.TaskData.TaskDetails.ActualOwner = t.Owner
		task.TaskData.TaskDetails.CreatedBy = i.User + " " + i.Role
		task.TaskData.TaskDetails.CreatedTime = effectiveTime
		task.TaskData.TaskDetails.RenderingMethodExists = "false"
		task.TaskData.TaskDetails.LastModifiedTime = effectiveTime
		task.TaskData.Description = t.Description
		task.TaskData.TaskDetails.Status = tukcnst.READY
		for _, inp := range t.Input {
			log.Println("Creating Task Input " + inp.Name)
			docinput := Input{}
			docinput.Part.Name = inp.Name
			docinput.Part.AttachmentInfo.Name = inp.Name
			docinput.Part.AttachmentInfo.AccessType = inp.AccessType
			docinput.Part.AttachmentInfo.ContentType = inp.Contenttype
			docinput.Part.AttachmentInfo.ContentCategory = tukcnst.MEDIA_TYPES
			task.TaskData.Input = append(task.TaskData.Input, docinput)
		}
		for _, outp := range t.Output {
			log.Println("Creating Task Output " + outp.Name)
			docoutput := Output{}
			docoutput.Part.Name = outp.Name
			docoutput.Part.AttachmentInfo.Name = outp.Name
			docoutput.Part.AttachmentInfo.AccessType = outp.AccessType
			docoutput.Part.AttachmentInfo.ContentType = outp.Contenttype
			docoutput.Part.AttachmentInfo.ContentCategory = tukcnst.MEDIA_TYPES
			task.TaskData.Output = append(task.TaskData.Output, docoutput)
		}
		log.Println("Creating New Workflow Task Event 'Create_Task' " + tevidstr)
		tev := TaskEvent{}
		tev.EventTime = effectiveTime
		tev.ID = tevidstr
		tev.Identifier = tevidstr
		tev.EventType = "Create_Task"
		tev.Status = tukcnst.COMPLETE
		log.Println("Set Workflow Task Event 'Create_Task' " + tevidstr + " status to " + tukcnst.COMPLETE)
		task.TaskEventHistory.TaskEvent = append(task.TaskEventHistory.TaskEvent, tev)
		i.XDWDocument.TaskList.XDWTask = append(i.XDWDocument.TaskList.XDWTask, task)
	}
	docevent := DocumentEvent{}
	docevent.Author = i.User + i.Role
	docevent.TaskEventIdentifier = tevidstr
	docevent.EventTime = effectiveTime
	docevent.EventType = "New_Workflow"
	docevent.PreviousStatus = "CREATED"
	docevent.ActualStatus = "READY"
	log.Printf("Set Workflow Document Event %s 'New_Workflow' status to %s", tevidstr, tukcnst.READY)
	i.XDWDocument.WorkflowStatusHistory.DocumentEvent = append(i.XDWDocument.WorkflowStatusHistory.DocumentEvent, docevent)
	log.Printf("Created new %s Workflow for Patient %s", i.XDWDocument.WorkflowDefinitionReference, i.NHS_ID)
	i.Response, _ = xml.MarshalIndent(i.XDWDocument, "", "  ")
	i.XDWVersion = 0
	i.persistWorkflow()
	return nil
}
func (i *XDWTransaction) persistWorkflow() error {
	var err error
	wfs := tukdbint.Workflows{Action: tukcnst.INSERT}
	wf := tukdbint.Workflow{
		XDW_Key: strings.ToUpper(i.Pathway) + i.NHS_ID,
		XDW_UID: i.XDWDocument.ID.Extension,
		Version: i.XDWVersion,
	}
	xdwDocBytes, _ := json.Marshal(i.XDWDocument)
	xdwDefBytes, _ := json.Marshal(i.XDWDefinition)
	wf.XDW_Doc = string(xdwDocBytes)
	wf.XDW_Def = string(xdwDefBytes)
	wfs.Workflows = append(wfs.Workflows, wf)
	if err = tukdbint.NewDBEvent(&wfs); err != nil {
		log.Println(err.Error())
	} else {
		log.Printf("Persisted Workflow Version %v for Pathway %s NHS ID %s", i.XDWVersion, i.Pathway, i.NHS_ID)
	}
	return err
}
func (i *XDWTransaction) newEventID() int64 {
	ev := tukdbint.Event{
		DocName:      i.XDWDocument.WorkflowDefinitionReference + "-" + i.NHS_ID,
		ClassCode:    i.XDSDocumentMeta.Classcode,
		ConfCode:     i.XDSDocumentMeta.Confcode,
		FormatCode:   i.XDSDocumentMeta.Formatcode,
		FacilityCode: i.XDSDocumentMeta.Facilitycode,
		PracticeCode: i.XDSDocumentMeta.Practicesettingcode,
		Authors:      i.XDWDocument.Author.AssignedAuthor.AssignedPerson.Name.Family + " " + i.XDWDocument.Author.AssignedAuthor.AssignedPerson.Name.Prefix,
		NhsId:        i.NHS_ID,
		User:         i.User,
		Org:          i.Org,
		Role:         i.Role,
		Pathway:      i.Pathway,
		Topic:        tukcnst.DSUB_TOPIC_TYPE_CODE,
		Notes:        string(i.Request),
		Version:      "0",
	}
	evs := tukdbint.Events{Action: tukcnst.INSERT}
	evs.Events = append(evs.Events, ev)
	if err := tukdbint.NewDBEvent(&evs); err != nil {
		log.Println(err.Error())
		return 0
	}
	log.Printf("Created Event ID :  = %v", evs.LastInsertId)
	return evs.LastInsertId
}
func getLocalId(mid string) string {
	idmaps := tukdbint.IdMaps{Action: tukcnst.SELECT}
	idmap := tukdbint.IdMap{Mid: mid}
	idmaps.LidMap = append(idmaps.LidMap, idmap)
	if err := tukdbint.NewDBEvent(&idmaps); err != nil {
		log.Println(err.Error())
		return mid
	}
	if idmaps.Cnt == 1 {
		return idmaps.LidMap[1].Lid
	}
	return mid
}
func getMappedId(lid string) string {
	idmaps := tukdbint.IdMaps{Action: tukcnst.SELECT}
	idmap := tukdbint.IdMap{Lid: lid}
	idmaps.LidMap = append(idmaps.LidMap, idmap)
	if err := tukdbint.NewDBEvent(&idmaps); err != nil {
		log.Println(err.Error())
		return lid
	}
	if idmaps.Cnt == 1 {
		return idmaps.LidMap[1].Mid
	}
	return lid
}
