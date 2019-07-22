package pc_pdf_generator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/appengine/file"
	"google.golang.org/appengine/memcache"
	"google.golang.org/appengine/urlfetch"

	"golang.org/x/net/context"
)

type PCDownloader struct {
	host          string
	credentialUrl string
	profileUrl    string
	fieldUrl      string
	domain        string
	authUrl       string
	listUrl       string
	peopleUrl     string
	clientId      string
	clientSecret  string
	tokenSecret   string
	token         string
	ctx           context.Context

	throttle       <-chan time.Time
	throttleActive bool
	wg             sync.WaitGroup
}

type Person struct {
	Id        string
	FirstName string
	LastName  string

	Address1   string
	Address2   string
	City       string
	State      string
	Country    string
	PostalCode string

	DateJoined time.Time
	Birthday   time.Time

	HomePhone int64
	CellPhone int64
	WorkPhone int64

	EmailAddress string

	Thumbnail bool

	NewMember90    bool
	PendingBaptism bool

	Occupation string

	Children1 string
	Children2 string

	Married bool

	Title    string
	Employer string
	School   string

	DirectorySections map[string]bool
}

type Household struct {
	Id       string
	SortKey  string
	Members  []*Person
	Children map[string]*Person
	Head     *Person
}

type PCTokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	Scope        string `json:"scope"`
	CreatedAt    int64  `json:"created_at"`
}

type PCOrganizationResponse struct {
	Meta struct {
		Parent struct {
			Id string `json:"id"`
		} `json:"parent"`
	} `json:"meta"`
}

type PCListResponse struct {
	Included []struct {
		Id int `json:"id,string"`
	} `json:"included"`
	Meta struct {
		TotalCount int `json:"total_count"`
		Count      int `json:"count"`
		Next       struct {
			Offset int `json:"offset"`
		} `json:"next,omitempty"`
	} `json:"meta"`
}

type PCFieldResponse struct {
	Data []struct {
		Id         string `json:"id"`
		Attributes struct {
			Name string `json:"name"`
		} `json:"attributes"`
	} `json:"data"`
}

type PCAddress struct {
	Id      string
	City    string
	State   string
	Street1 string
	Street2 string
	Zip     string
}

type PCPeopleResponse struct {
	Included []struct {
		Id    string `json:"id"`
		Type  string `json:"type"`
		Links struct {
			Self string `json:"self"`
		} `json:"links"`
		Attributes struct {
			City             string `json:"city"`
			Location         string `json:"location"`
			Primary          bool   `json:"primary"`
			State            string `json:"state"`
			Street           string `json:"street"`
			Zip              string `json:"zip"`
			PrimaryContactId string `json:"primary_contact_id"`
			Address          string `json:"address"`
			Number           string `json:"number"`
			Value            string `json:"value"`
			Birthdate        string `json:"birthdate"`
			IsChild          bool   `json:"child"`
			FirstName        string `json:"first_name"`
			LastName         string `json:"last_name"`
			MiddleName       string `json:"middle_name"`
			NickName         string `json:"nickname"`
		} `json:"attributes"`
		Relationships struct {
			Person struct {
				Data struct {
					Type string `json:"type"`
					Id   string `json:"id"`
				} `json:"data"`
			} `json:"person"`
			FieldDefinition struct {
				Data struct {
					Type string `json:"type"`
					Id   string `json:"id"`
				} `json:"data"`
			} `json:"field_definition"`
		} `json:"relationships"`
	} `json:"included"`
	Data PCPersonResponse `json:"data"`
	Meta struct {
		TotalCount int `json:"total_count"`
		Count      int `json:"count"`
		Next       struct {
			Offset int `json:"offset"`
		} `json:"next,omitempty"`
	} `json:"meta"`
}

type PCPersonResponse struct {
	Id         string `json:"id"`
	Attributes struct {
		Avatar     string `json:"avatar"`
		Birthdate  string `json:"birthdate"`
		IsChild    bool   `json:"child"`
		FirstName  string `json:"first_name"`
		LastName   string `json:"last_name"`
		MiddleName string `json:"middle_name"`
		Status     string `json:"status"`
		NickName   string `json:"nickname"`
	} `json:"attributes"`
	Relationships struct {
		Addresses struct {
			Data []struct {
				Id string `json:"id"`
			} `json:"data"`
		} `json:"addresses"`
		Emails struct {
			Data []struct {
				Id string `json:"id"`
			} `json:"data"`
		} `json:"emails"`
		PhoneNumbers struct {
			Data []struct {
				Id string `json:"id"`
			} `json:"data"`
		} `json:"phone_numbers"`
		FieldData struct {
			Data []struct {
				Id string `json:"id"`
			} `json:"data"`
		} `json:"field_data"`
		Households struct {
			Data []struct {
				Id string `json:"id"`
			} `json:"data"`
		} `json:"households"`
	} `json:"relationships"`
}

const (
	timeFormat  = "2006-01-02"
	timeFormat2 = "01/02/2006"
)

// PC Integration
func (dl *PCDownloader) CheckSession(token string, refreshToken string, expiration int64) (err error, newToken string, newRefreshToken string, newExpiration int64, domain string) {
	newToken = token
	newRefreshToken = refreshToken
	newExpiration = expiration

	if time.Now().Unix() >= newExpiration {
		log.Printf("Getting code\n")
		err, newToken, newRefreshToken, newExpiration := dl.GetTokens(refreshToken)

		if err != nil {
			log.Printf("Error getting token: %s", err)
			return err, newToken, newRefreshToken, newExpiration, domain
		}
	}

	req, err := http.NewRequest("GET", dl.profileUrl, nil)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", newToken))

	client := urlfetch.Client(dl.ctx)
	var resp *http.Response
	err = retry(2, 1*time.Second, func() (err error) {
		resp, err = client.Do(req)
		return
	})
	if err != nil {
		log.Printf("Error getting token: %s", err)
		return err, newToken, newRefreshToken, newExpiration, domain
	}
	defer resp.Body.Close()

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error getting token: %s", err)
		return err, newToken, newRefreshToken, newExpiration, domain
	}

	orgData := PCOrganizationResponse{}

	json.Unmarshal(contents, &orgData)

	domain = orgData.Meta.Parent.Id

	return err, newToken, newRefreshToken, newExpiration, domain
}

func (dl *PCDownloader) GetTokens(code string) (err error, token string, refreshToken string, expiration int64) {
	postBody := map[string]interface{}{
		"grant_type":    "authorization_code",
		"code":          code,
		"client_id":     dl.clientId,
		"client_secret": dl.clientSecret,
		"redirect_uri":  dl.authUrl,
	}

	jsonStr, err := json.Marshal(postBody)
	if err != nil {
		return err, token, refreshToken, expiration
	}

	req, err := http.NewRequest("POST", dl.credentialUrl, bytes.NewBuffer(jsonStr))
	if err != nil {
		return err, token, refreshToken, expiration
	}
	req.Header.Set("Content-Type", "application/json")

	client := urlfetch.Client(dl.ctx)

	var resp *http.Response
	err = retry(2, 1*time.Second, func() (err error) {
		resp, err = client.Do(req)
		return
	})
	if err != nil {
		return err, token, refreshToken, expiration
	}
	defer resp.Body.Close()

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error getting token: %s", err)
		return err, token, refreshToken, expiration
	}

	tokenData := PCTokenResponse{}

	json.Unmarshal(contents, &tokenData)

	return err, tokenData.AccessToken, tokenData.RefreshToken, (tokenData.CreatedAt + tokenData.ExpiresIn)
}

func (dl *PCDownloader) downloadImage(remoteUrl string, person *Person, retryCount int) (err error) {
	defer dl.wg.Done()

	if strings.Contains(remoteUrl, "svg") {
		return err
	}

	bucketName, err := file.DefaultBucketName(dl.ctx)
	if err != nil {
		return err
	}

	client, err := storage.NewClient(dl.ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)

	_, err = bucket.Object(dl.domain + "/jpgs/" + person.Id).Attrs(dl.ctx)
	//change this to err == nil when we want to turn on caching again
	if err == nil {
		person.Thumbnail = true
		return err
	}

	contents, err := dl.downloadContent(remoteUrl)
	if err != nil {
		return err
	}

	if len(contents) < 1 && retryCount < 6 {
		dl.wg.Add(1)
		go dl.downloadImage(remoteUrl, person, retryCount+1)
		return err
	}

	inputBytes := bytes.NewReader(contents)
	input, _, err := image.Decode(inputBytes)
	if err != nil {
		log.Printf("%s\n", err)
		return err
	}

	wc := bucket.Object(dl.domain + "/jpgs/" + person.Id).NewWriter(dl.ctx)
	wc.ContentType = "image/jpeg"

	defer wc.Close()

	err = jpeg.Encode(wc, input, nil)
	if err != nil {
		log.Printf("%s\n", err)
		return err
	}

	person.Thumbnail = true

	input = nil
	inputBytes = nil

	return err
}

func (dl *PCDownloader) downloadHousehold(remoteUrl string, household *Household) (err error) {
	defer dl.wg.Done()
	contents, err := dl.downloadContent(remoteUrl + "?include=people")
	if err != nil {
		return err
	}

	res := PCPeopleResponse{}
	json.Unmarshal(contents, &res)

	householdMap := household.Children

	for _, v := range res.Included {
		if v.Attributes.IsChild {
			person := &Person{
				FirstName: v.Attributes.FirstName,
				LastName:  v.Attributes.LastName,
				Id:        v.Id,
			}
			t2, _ := time.Parse(timeFormat, v.Attributes.Birthdate)
			person.Birthday = t2

			if v.Attributes.NickName != "" {
				person.FirstName = v.Attributes.NickName
			}

			householdMap[v.Id] = person
		}
	}

	return err
}

func (dl *PCDownloader) downloadList(listName string) (households map[string]Household, err error) {
	households = make(map[string]Household)
	offset := 0
	dataRemaining := true

	for dataRemaining {
		households, offset, dataRemaining, err = dl.downloadListPage(listName, households, offset)
	}

	dl.wg.Wait()

	return households, err
}

func (dl *PCDownloader) downloadListPage(listName string, prevHouseholds map[string]Household, oldOffset int) (households map[string]Household, offset int, dataRemaining bool, err error) {
	dataRemaining = false
	remoteUrl := fmt.Sprintf("%s?include=people&per_page=100&offset=%d&where[name]=%s", dl.listUrl, offset, url.QueryEscape(listName))

	contents, err := dl.downloadContent(remoteUrl)
	if err != nil {
		return households, offset, dataRemaining, err
	}

	res := PCListResponse{}
	json.Unmarshal(contents, &res)

	var peopleIds []int

	for _, v := range res.Included {
		peopleIds = append(peopleIds, v.Id)
	}

	households = prevHouseholds
	households, err = dl.downloadPeople(peopleIds, households)

	if res.Meta.Next.Offset > 0 {
		dataRemaining = true
		offset = res.Meta.Next.Offset
	}

	return households, offset, dataRemaining, err
}

func (dl *PCDownloader) downloadPeople(peopleIds []int, prevHouseholds map[string]Household) (households map[string]Household, err error) {
	for _, i := range peopleIds {
		households, err = dl.downloadPerson(i, prevHouseholds)
	}

	dl.wg.Wait()

	return households, err
}

func (dl *PCDownloader) downloadContent(remoteUrl string) (contents []byte, err error) {
	item, err := memcache.Get(dl.ctx, remoteUrl)

	dl.throttle = time.Tick(time.Second / 4)
	dl.throttleActive = true

	// if !dl.throttleActive {
	// 	dl.throttle = time.Tick(time.Second / 3)
	// 	dl.throttleActive = true
	// }

	if err != nil {
		if err != memcache.ErrCacheMiss {
			log.Printf("Err: %s - %s \n", err, remoteUrl)
		}
		err = nil
		<-dl.throttle

		req, err := http.NewRequest("GET", remoteUrl, nil)
		if err != nil {
			return contents, err
		}

		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", dl.token))
		req.Header.Set("Accept", "application/json")

		client := urlfetch.Client(dl.ctx)
		var resp *http.Response
		err = retry(5, 1*time.Second, func() (err error) {
			resp, err = client.Do(req)
			return
		})
		if err != nil {
			return contents, err
		}
		defer resp.Body.Close()

		contents, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return contents, err
		}

		memcache.Set(dl.ctx, &memcache.Item{Key: remoteUrl, Value: contents, Expiration: cacheTTL})
	} else {
		contents = item.Value
	}

	return contents, err
}

func (dl *PCDownloader) getFieldDefinitions() (fieldDefinitions map[string]string, err error) {
	remoteUrl := dl.fieldUrl
	fieldDefinitions = make(map[string]string)

	contents, err := dl.downloadContent(remoteUrl)
	if err != nil {
		return fieldDefinitions, err
	}

	res := PCFieldResponse{}
	json.Unmarshal(contents, &res)

	for _, v := range res.Data {
		fieldDefinitions[v.Id] = v.Attributes.Name
	}
	log.Println(fieldDefinitions)
	return fieldDefinitions, err
}

func (dl *PCDownloader) downloadPerson(peopleId int, prevHouseholds map[string]Household) (households map[string]Household, err error) {
	remoteUrl := fmt.Sprintf("%s/%d?include=addresses,emails,phone_numbers,field_data,households,marital_status", dl.peopleUrl, peopleId)

	contents, err := dl.downloadContent(remoteUrl)
	if err != nil {
		return households, err
	}

	res := PCPeopleResponse{}
	json.Unmarshal(contents, &res)

	var email string
	var mobilePhone int64
	var workPhone int64
	var homePhone int64
	var married bool
	var householdId string
	var householdHead string
	var householdLink string
	var address PCAddress

	households = prevHouseholds

	if res.Data.Attributes.IsChild {
		return households, err
	}

	fieldDefinitions, err := dl.getFieldDefinitions()
	if err != nil {
		return households, err
	}

	fieldData := make(map[string]string)

	for _, v := range res.Included {
		peopleId := v.Relationships.Person.Data.Id
		if v.Type == "Address" {
			if address.Id == "" || v.Attributes.Primary {
				streetParts := strings.Split(v.Attributes.Street, "\n")

				address = PCAddress{
					Id:      peopleId,
					Street1: streetParts[0],
					City:    v.Attributes.City,
					State:   v.Attributes.State,
					Zip:     v.Attributes.Zip,
				}

				if len(streetParts) > 1 {
					address.Street2 = streetParts[1]
				}
			}
		}

		if v.Type == "Email" {
			if email == "" || v.Attributes.Primary {
				email = v.Attributes.Address
			}
		}

		if v.Type == "Household" {
			householdId = v.Id
			householdHead = v.Attributes.PrimaryContactId
			householdLink = v.Links.Self
		}

		if v.Type == "FieldDatum" {
			fieldData[fieldDefinitions[v.Relationships.FieldDefinition.Data.Id]] = v.Attributes.Value
		}

		if v.Type == "PhoneNumber" {
			if mobilePhone == 0 || v.Attributes.Primary {
				mobilePhone = extractDigits(v.Attributes.Number)
			}
			// if v.Attributes.Location == "Mobile" {
			// 	if mobilePhone == 0 || v.Attributes.Primary {
			// 		mobilePhone = extractDigits(v.Attributes.Number)
			// 	}
			// }

			// if v.Attributes.Location == "Home" {
			// 	if homePhone == 0 || v.Attributes.Primary {
			// 		homePhone = extractDigits(v.Attributes.Number)
			// 	}
			// }

			// if v.Attributes.Location == "Work" {
			// 	if workPhone == 0 || v.Attributes.Primary {
			// 		workPhone = extractDigits(v.Attributes.Number)
			// 	}
			// }
		}

		if v.Type == "MaritalStatus" {
			if v.Attributes.Value == "Married" {
				married = true
			}
		}
	}

	if households[householdId].Id == "" {
		households[householdId] = Household{
			Id:       householdId,
			Members:  make([]*Person, 0),
			Children: make(map[string]*Person),
		}
	}

	v := res.Data

	household := households[householdId]

	person := Person{
		FirstName:  v.Attributes.FirstName,
		LastName:   v.Attributes.LastName,
		Id:         v.Id,
		Occupation: fieldData["Occupation"],
		Children1:  fieldData["Line 1 Children (Directory Use)"],
		Children2:  fieldData["Line 2 Children (Directory Use)"],
		School:     fieldData["School"],
		Employer:   fieldData["Employer"],
		Title:      fieldData["Title"],
	}

	if v.Attributes.Avatar != "" {
		dl.wg.Add(1)
		go dl.downloadImage(v.Attributes.Avatar, &person, 0)
	}

	if householdLink != "" {
		dl.wg.Add(1)
		go dl.downloadHousehold(householdLink, &household)
	}

	if v.Attributes.NickName != "" {
		person.FirstName = v.Attributes.NickName
	}

	person.Address1 = address.Street1
	person.Address2 = address.Street2
	person.City = address.City
	person.State = address.State
	person.PostalCode = address.Zip
	person.Country = "US"

	person.EmailAddress = email

	person.HomePhone = homePhone
	person.WorkPhone = workPhone
	person.CellPhone = mobilePhone

	person.Married = married

	t2, _ := time.Parse(timeFormat, v.Attributes.Birthdate)
	person.Birthday = t2

	t, _ := time.Parse(timeFormat2, fieldData["Date Joined"])
	person.DateJoined = t
	person.NewMember90 = t.Sub(time.Now()).Hours()/24 > -91

	if fieldData["Baptism Date"] == "" {
		person.PendingBaptism = true
	}

	if householdHead == person.Id {
		household.Head = &person
		household.SortKey = person.LastName + person.FirstName + person.Id
	} else {
		household.Members = append(household.Members, &person)

		if household.SortKey == "" {
			household.SortKey = person.LastName + person.FirstName + person.Id
		}
	}

	households[householdId] = household

	return households, err
}

func retry(attempts int, sleep time.Duration, callback func() error) (err error) {
	for i := 0; ; i++ {
		err = callback()
		if err == nil {
			return
		}

		if i >= (attempts - 1) {
			break
		}

		time.Sleep(sleep)

		log.Println("retrying after error:", err)
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}

func extractDigits(str string) (value int64) {
	re := regexp.MustCompile("[0-9]+")
	value, _ = strconv.ParseInt(strings.Join(re.FindAllString(str, -1), ""), 10, 64)
	return value
}

func arrayToString(a []int) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", ",", -1), "[]")
}
