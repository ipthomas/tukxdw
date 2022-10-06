package tukxdw

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"tukcnst"
	"tukdb-readwriter/tuk"
	"tukdbint"

	"github.com/ipthomas/tukutil"
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

var (
	TUK_DB_URL        = ""
	DSUB_BROKER_URL   = ""
	DSUB_CONSUMER_URL = ""
)

func InitOSvars() {
	TUK_DB_URL = os.Getenv("TUK_DB_URL")
	DSUB_BROKER_URL = os.Getenv("DSUB_BROKER_URL")
	DSUB_CONSUMER_URL = os.Getenv("DSUB_CONSUMER_URL")

	if TUK_DB_URL == "" {
		TUK_DB_URL = "https://5k2o64mwt5.execute-api.eu-west-1.amazonaws.com/beta/"
	}
	if DSUB_CONSUMER_URL == "" {
		DSUB_CONSUMER_URL = "https://cjrvrddgdh.execute-api.eu-west-1.amazonaws.com/beta/"
	}
	if DSUB_BROKER_URL == "" {
		DSUB_BROKER_URL = "http://spirit-test-01.tianispirit.co.uk:8081/SpiritXDSDsub/Dsub"
	}
}
func RegisterWorkflowDefinition(wfdef string) error {
	//[pwy]pwyExpressions
	brokerExps := make(map[string]map[string]string)
	xdwdefs := make(map[string]WorkflowDefinition)
	if xdwdef, updated := updateXDWDefinition(wfdef); updated {
		xdwdefs[xdwdef.Ref] = xdwdef
		//[expression]pathway
		pwyExpressions := make(map[string]string)
		log.Println("Parsing XDW Tasks for potential DSUB Broker Subscriptions")
		for _, task := range xdwdef.Tasks {
			for _, inp := range task.Input {
				log.Printf("Checking Input Task %s", inp.Name)
				if inp.AccessType == tukcnst.XDS_REGISTERED {
					pwyExpressions[inp.Name] = xdwdef.Ref
					log.Printf("Task %v %s task input %s included in potential DSUB Broker subscriptions", task.ID, task.Name, inp.Name)
				} else {
					log.Printf("Input Task %s does not require a dsub broker subscription", inp.Name)
				}
			}
			for _, out := range task.Output {
				log.Printf("Checking Output Task %s", out.Name)
				if out.AccessType == tukcnst.XDS_REGISTERED {
					pwyExpressions[out.Name] = xdwdef.Ref
					log.Printf("Task %v %s task output %s included in potential DSUB Broker subscriptions", task.ID, task.Name, out.Name)
				} else {
					log.Printf("Output Task %s does not require a dsub broker subscription", out.Name)
				}
			}
		}
		brokerExps[xdwdef.Ref] = pwyExpressions
	}
	log.Printf("Found %v potential DSUB Broker Subscriptions - %s", len(brokerExps), brokerExps)
	for pwy, pwyExpressions := range brokerExps {
		sub := tukdbint.Subscription{Pathway: strings.ToUpper(pwy)}
		subs := tukdbint.Subscriptions{Action: "delete"}
		subs.Subscriptions = append(subs.Subscriptions, sub)
		if err := tukdbint.NewDBEvent(&subs); err != nil {
			log.Println(err.Error())
			return err
		}
		for exp, pwy := range pwyExpressions {
			subscribe := DSUBSubscribe{
				Topic:      tukcnst.DSUB_TOPIC_TYPE_CODE,
				Expression: exp,
			}
			if err := subscribe.NewEvent(); err != nil {
				log.Println(err.Error())
				return err
			}
			tuksub := tukdbint.Subscription{
				BrokerRef:  subscribe.BrokerRef,
				Pathway:    strings.ToUpper(pwy),
				Topic:      tukcnst.DSUB_TOPIC_TYPE_CODE,
				Expression: exp,
			}
			tuksubs := tukdbint.Subscriptions{
				Action: "insert",
			}
			tuksubs.Subscriptions = append(tuksubs.Subscriptions, tuksub)
			if err := tukdbint.NewDBEvent(&tuksubs); err != nil {
				log.Println(err.Error())
				return err
			}
		}
	}
	return nil
}
func (i *DSUBSubscribe) NewEvent() error {
	i.UUID = tukutil.NewUuid()
	i.BrokerUrl = DSUB_BROKER_URL
	i.ConsumerUrl = DSUB_CONSUMER_URL
	reqMsg := "{{define \"subscribe\"}}<SOAP-ENV:Envelope xmlns:SOAP-ENV='http://www.w3.org/2003/05/soap-envelope' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns:s='http://www.w3.org/2001/XMLSchema' xmlns:wsa='http://www.w3.org/2005/08/addressing'><SOAP-ENV:Header><wsa:Action SOAP-ENV:mustUnderstand='true'>http://docs.oasis-open.org/wsn/bw-2/NotificationProducer/SubscribeRequest</wsa:Action><wsa:MessageID>urn:uuid:{{.UUID}}</wsa:MessageID><wsa:ReplyTo SOAP-ENV:mustUnderstand='true'><wsa:Address>http://www.w3.org/2005/08/addressing/anonymous</wsa:Address></wsa:ReplyTo><wsa:To>{{.BrokerUrl}}</wsa:To></SOAP-ENV:Header><SOAP-ENV:Body><wsnt:Subscribe xmlns:wsnt='http://docs.oasis-open.org/wsn/b-2' xmlns:a='http://www.w3.org/2005/08/addressing' xmlns:rim='urn:oasis:names:tc:ebxml-regrep:xsd:rim:3.0' xmlns:wsa='http://www.w3.org/2005/08/addressing'><wsnt:ConsumerReference><wsa:Address>{{.ConsumerUrl}}</wsa:Address></wsnt:ConsumerReference><wsnt:Filter><wsnt:TopicExpression Dialect='http://docs.oasis-open.org/wsn/t-1/TopicExpression/Simple'>ihe:FullDocumentEntry</wsnt:TopicExpression><rim:AdhocQuery id='urn:uuid:742790e0-aba6-43d6-9f1f-e43ed9790b79'><rim:Slot name='{{.Topic}}'><rim:ValueList><rim:Value>('{{.Expression}}')</rim:Value></rim:ValueList></rim:Slot></rim:AdhocQuery></wsnt:Filter></wsnt:Subscribe></SOAP-ENV:Body></SOAP-ENV:Envelope>{{end}}"
	tmplt, err := template.New(tukcnst.SUBSCRIBE).Parse(reqMsg)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	var b bytes.Buffer
	err = tmplt.Execute(&b, i)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	i.Request = b.Bytes()
	err = i.createSubscription()
	if err != nil {
		log.Println(err.Error())
	}
	return err
}
func (i *DSUBSubscribe) createSubscription() error {
	log.Printf("Sending DSUB Subscribe request to %s", i.BrokerUrl)
	reqStr := string(i.Request)
	log.Println(reqStr)
	var resp *http.Response
	req, err := http.NewRequest(http.MethodPost, i.BrokerUrl, strings.NewReader(reqStr))
	if err != nil {
		log.Println(err.Error())
		return err
	}
	req.Header.Set(tukcnst.SOAP_ACTION, tukcnst.SOAP_ACTION_SUBSCRIBE_REQUEST)
	req.Header.Set(tukcnst.CONTENT_TYPE, tukcnst.SOAP_XML)
	req.Header.Set(tukcnst.ACCEPT, tukcnst.ALL)
	req.Header.Set(tukcnst.CONNECTION, tukcnst.KEEP_ALIVE)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	rsp, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	subrsp := DSUBSubscribeResponse{}
	log.Println("DSUB Broker Response")
	log.Println(string(rsp))
	if err := xml.Unmarshal(rsp, &subrsp); err != nil {
		log.Println(err.Error())
	}
	i.BrokerRef = subrsp.Body.SubscribeResponse.SubscriptionReference.Address
	if i.BrokerRef == "" {
		log.Println("No Broker Reference found in response message from DSUB Broker")
		err = errors.New("no broker reference found in response message from dsub broker")
	} else {
		log.Printf("Broker Response. Broker Ref :  %s", subrsp.Body.SubscribeResponse.SubscriptionReference.Address)
	}
	return err
}
func updateXDWDefinition(wfdef string) (WorkflowDefinition, bool) {
	xdwdef := WorkflowDefinition{}
	xdwfile, err := os.Open(Basepath + "/xdwconfig/" + file.Name())
	if err != nil {
		log.Println(err.Error())
		return xdwdef, false
	}
	json.NewDecoder(xdwfile).Decode(&xdwdef)
	log.Println("Loaded WF Def for Pathway : " + xdwdef.Ref)

	xdw := tuk.XDW{Name: xdwdef.Ref}
	xdws := tuk.XDWS{Action: tukcnst.DELETE}
	xdws.XDW = append(xdws.XDW, xdw)
	if err := xdws.NewEvent(); err != nil {
		log.Println(err.Error())
		return xdwdef, false
	}
	xdwBytes, _ := json.Marshal(xdwdef)
	xdw = tuk.XDW{Name: xdwdef.Ref, IsXDSMeta: false, XDW: string(xdwBytes)}
	xdws = tuk.XDWS{Action: tukcnst.INSERT}
	xdws.XDW = append(xdws.XDW, xdw)
	if err := xdws.NewEvent(); err != nil {
		log.Println(err.Error())
		return xdwdef, false
	}
	return xdwdef, true
}
func (i *Subscriptions) NewEvent() error {
	log.Printf("Sending Request to %s", TUK_DB_URL+tukcnst.SUBSCRIPTIONS)
	body, _ := json.Marshal(i)
	bodyBytes, err := newTUKDBRequest(tukcnst.SUBSCRIPTIONS, body)
	if err == nil {
		if err := json.Unmarshal(bodyBytes, &i); err != nil {
			fmt.Println(err.Error())
		}
	}
	return err
}
func newTUKDBRequest(resource string, body []byte) ([]byte, error) {
	if TUK_DB_URL == "" {
		TUK_DB_URL = "https://5k2o64mwt5.execute-api.eu-west-1.amazonaws.com/beta/"
	}
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodPost, TUK_DB_URL+resource, bytes.NewBuffer(body))
	if err != nil {
		log.Println(err)
		return nil, err
	}
	req.Header.Add(tukcnst.CONTENT_TYPE, tukcnst.APPLICATION_JSON_CHARSET_UTF_8)
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	log.Printf("Response Status Code %v\n", resp.StatusCode)
	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
		} else {
			return bodyBytes, nil
		}
	}
	return nil, err
}
