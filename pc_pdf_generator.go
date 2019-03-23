package pc_pdf_generator

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"

	"github.com/dsoprea/goappenginesessioncascade"
	"github.com/julienschmidt/httprouter"
)

const (
	sessionName   = "s"
	clientId      = configClientID
	clientSecret  = configClientSecret
	devAuthUrl    = "http://%s/api/v1/authorize"
	authUrl       = "https://%s/api/v1/authorize"
	fieldUrl      = "https://api.planningcenteronline.com/people/v2/field_definitions?per_page=100"
	listUrl       = "https://api.planningcenteronline.com/people/v2/lists"
	peopleUrl     = "https://api.planningcenteronline.com/people/v2/people"
	credentialUrl = "https://api.planningcenteronline.com/oauth/token"
	profileUrl    = "https://api.planningcenteronline.com/people/v2/me"
	hostPattern   = "https://api.planningcenteronline.com/oauth/authorize?client_id=%s&redirect_uri=%s&response_type=code&scope=people"
	cacheTTL      = time.Duration(5) * time.Minute
	hostName      = "hinson-dot-directory-export-pdf.appspot.com"
)

type Config struct {
	Id               int64     `json:"id"`
	TopMargin        float64   `json:"top_margin,string"`
	LeftMargin       float64   `json:"left_margin,string"`
	BottomMargin     float64   `json:"bottom_margin,string"`
	RightMargin      float64   `json:"right_margin,string"`
	PageSize         string    `json:"page_size"`
	NumberOfColumns  float64   `json:"number_of_columns,string"`
	Padding          float64   `json:"padding,string"`
	Gutter           float64   `json:"gutter,string"`
	ImagePadding     float64   `json:"image_padding,string"`
	ColumnHeight     float64   `json:"column_height,string"`
	FontSize         float64   `json:"font_size,string"`
	FontFamily       string    `json:"font_family"`
	LineHeight       float64   `json:"line_height,string"`
	HighlightOpacity float64   `json:"highlight_opacity,string"`
	Sections         []Section `json:"sections"`
}

type Overrides struct {
	Overrides []Section `json:"overrides"`
}

type Section struct {
	KeyFirstName       string   `json:"key_first_name"`
	KeyLastName        string   `json:"key_last_name"`
	KeyBirthday        string   `json:"key_birthday"`
	ShowHousehold      bool     `json:"show_household"`
	ShowChildren       bool     `json:"show_children"`
	Show               bool     `json:"show"`
	Header             string   `json:"header"`
	ListName           string   `json:"list_name"`
	ExcludeDirSections []string `json:"exclude_dir_sections"`
	PhoneCount         int      `json:"phone_count,string"`
	JobTitle           bool     `json:"job_title"`
	Employer           bool     `json:"employer"`
	Occupation         bool     `json:"occupation"`
	Children           bool     `json:"children"`
	School             bool     `json:"school"`
	Age                bool     `json:"age"`
	Birthday           bool     `json:"birthday"`
	DateJoined         bool     `json:"date_joined"`
	Address            bool     `json:"address"`
	City               bool     `json:"city"`
	State              bool     `json:"state"`
	PostalCode         bool     `json:"postal_code"`
	Country            bool     `json:"country"`
	Email              bool     `json:"email"`
	Phones             bool     `json:"phones"`
	NewMemberFootnote  bool     `json:"new_member_footnote"`
	BaptismFootnote    bool     `json:"baptism_footnote"`
	LineSpacing        float64  `json:"line_spacing,string"`
	Columns            float64  `json:"columns,string"`
}

type ConfigRecord struct {
	Config []byte
}

type StatusRecord struct {
	Done  bool
	Error string
}

type OverridesRecord struct {
	Overrides []byte
}

var (
	authKey      = []byte("5b13524b25c0a2ee2b599d681715da4a")
	cryptKey     = []byte("3c48d034e14c8bb5905dd452ce4cfe64")
	sessionStore = cascadestore.NewCascadeStore(cascadestore.DistributedBackends, authKey, cryptKey)
)

func getSession(w http.ResponseWriter, r *http.Request) (pcDownloader PCDownloader) {
	ctx := appengine.NewContext(r)
	session, err := sessionStore.Get(r, sessionName)

	token, _ := session.Values["token"].(string)
	refreshToken, _ := session.Values["refreshToken"].(string)
	expiration, _ := session.Values["expiration"].(int64)

	if token == "" {
		redirectUrl := authUrl
		if appengine.IsDevAppServer() {
			redirectUrl = devAuthUrl
		}

		http.Redirect(w, r, fmt.Sprintf(hostPattern, clientId, fmt.Sprintf(redirectUrl, hostName)), http.StatusSeeOther)
		return pcDownloader
	}

	redirectUrl := authUrl
	if appengine.IsDevAppServer() {
		redirectUrl = devAuthUrl
	}

	pcDownloader = PCDownloader{
		clientId:      clientId,
		clientSecret:  clientSecret,
		credentialUrl: credentialUrl,
		profileUrl:    profileUrl,
		listUrl:       listUrl,
		peopleUrl:     peopleUrl,
		fieldUrl:      fieldUrl,
		authUrl:       fmt.Sprintf(redirectUrl, hostName),
		ctx:           ctx,
	}

	err, token, refreshToken, expiration, domain := pcDownloader.CheckSession(token, refreshToken, expiration)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return pcDownloader
	}

	session.Values["token"] = token
	session.Values["refreshToken"] = refreshToken
	session.Values["expiration"] = expiration

	err = session.Save(r, w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return pcDownloader
	}

	ctx, err = appengine.Namespace(ctx, domain)
	if err != nil || domain == "" || token == "" {
		log.Criticalf(ctx, "Failed to set namespace: %s\n", err)
		session.Values["token"] = nil
		session.Save(r, w)
		http.Redirect(w, r, "/", 303)
		panic(err)
	}

	pcDownloader.domain = domain
	pcDownloader.token = token
	pcDownloader.ctx = ctx

	return pcDownloader
}

func Index(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	getSession(w, r)

	t, _ := template.ParseFiles("js_app/index.html")
	err := t.Execute(w, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func Authorize(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	ctx := appengine.NewContext(r)
	code := r.URL.Query().Get("code")

	redirectUrl := authUrl
	if appengine.IsDevAppServer() {
		redirectUrl = devAuthUrl
	}

	PCDownloader := PCDownloader{
		clientId:      clientId,
		clientSecret:  clientSecret,
		credentialUrl: credentialUrl,
		authUrl:       fmt.Sprintf(redirectUrl, hostName),
		ctx:           ctx,
	}

	err, token, refreshToken, expiration := PCDownloader.GetTokens(code)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	session, err := sessionStore.Get(r, sessionName)

	session.Values["token"] = token
	session.Values["refreshToken"] = refreshToken
	session.Values["expiration"] = expiration

	err = session.Save(r, w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/", 303)
	return
}

func SaveConfig(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	pcDownloader := getSession(w, r)

	id, _ := strconv.ParseInt(params.ByName("id"), 10, 64)

	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	var config Config
	err := decoder.Decode(&config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	config.Id = id

	configKey := datastore.NewKey(pcDownloader.ctx, "Config", "", id, nil)
	configBytes, err := json.Marshal(config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = datastore.Put(pcDownloader.ctx, configKey, &ConfigRecord{Config: configBytes})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(config)
}

func SaveOverrides(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	pcDownloader := getSession(w, r)

	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	var overrides Overrides
	err := decoder.Decode(&overrides)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	overridesMap := make(map[string]Section)

	for _, override := range overrides.Overrides {
		key := strings.ToLower(fmt.Sprintf("%s-%s-%s", override.KeyFirstName, override.KeyLastName, override.KeyBirthday))
		overridesMap[key] = override
	}

	overrideBytes, err := json.Marshal(overridesMap)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	overridesKey := datastore.NewKey(pcDownloader.ctx, "Overrides", "", 1, nil)
	_, err = datastore.Put(pcDownloader.ctx, overridesKey, &OverridesRecord{Overrides: overrideBytes})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(overridesMap)
}

func GetOverrides(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	pcDownloader := getSession(w, r)

	var overrides map[string]Section

	overrideRecord := OverridesRecord{}
	overrideKey := datastore.NewKey(pcDownloader.ctx, "Overrides", "", 1, nil)
	err := datastore.Get(pcDownloader.ctx, overrideKey, &overrideRecord)
	if err != nil {
		log.Warningf(pcDownloader.ctx, "error pulling overrides: %s\n", err)
	}
	if overrideRecord.Overrides != nil {
		err := json.Unmarshal(overrideRecord.Overrides, &overrides)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(overrides)
}

func CreatePDF(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	pcDownloader := getSession(w, r)

	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	var config Config
	err := decoder.Decode(&config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	postValues := url.Values{}
	configJson, err := json.Marshal(config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	id := time.Now().Unix()

	postValues.Set("config", string(configJson))
	postValues.Set("token", pcDownloader.token)
	postValues.Set("domain", pcDownloader.domain)
	postValues.Set("fileId", fmt.Sprintf("%d", id))

	t := taskqueue.NewPOSTTask("/api/v1/workers/pdf", postValues)
	if _, err := taskqueue.Add(pcDownloader.ctx, t, ""); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	statusKey := datastore.NewKey(pcDownloader.ctx, "Status", "", id, nil)
	_, err = datastore.Put(pcDownloader.ctx, statusKey, &StatusRecord{})
	if err != nil {
		log.Errorf(pcDownloader.ctx, "error saving status: %s\n", err)
	}

	fmt.Fprintf(w, "{\"id\":\"%d\"}", id)
	return
}

func PDFWorker(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	ctx := appengine.NewContext(r)
	domain := r.FormValue("domain")
	token := r.FormValue("token")
	fileId := r.FormValue("fileId")

	ctx, err := appengine.Namespace(ctx, domain)
	if err != nil {
		log.Criticalf(ctx, "Failed to set namespace: %s\n", err)
		http.Error(w, "", http.StatusForbidden)
		return
	}

	decoder := json.NewDecoder(strings.NewReader(r.FormValue("config")))
	var config Config
	err = decoder.Decode(&config)
	if err != nil {
		log.Criticalf(ctx, "Failed to decode json: %s\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pcDownloader := PCDownloader{
		token:         token,
		domain:        domain,
		credentialUrl: credentialUrl,
		profileUrl:    profileUrl,
		listUrl:       listUrl,
		peopleUrl:     peopleUrl,
		fieldUrl:      fieldUrl,
		ctx:           ctx,
	}

	err = generatePDF(&config, pcDownloader, fileId)
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}

	id, _ := strconv.ParseInt(fileId, 10, 64)

	statusKey := datastore.NewKey(ctx, "Status", "", id, nil)
	_, keyErr := datastore.Put(ctx, statusKey, &StatusRecord{Done: true, Error: errStr})
	if keyErr != nil {
		log.Errorf(ctx, "error saving status: %s\n", keyErr)
		http.Error(w, keyErr.Error(), http.StatusInternalServerError)
		return
	}

	if err != nil {
		log.Errorf(ctx, "error generating PDF: %s\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}

func CheckPDF(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	pcDownloader := getSession(w, r)

	id, _ := strconv.ParseInt(params.ByName("id"), 10, 64)

	statusRecord := StatusRecord{}
	statusKey := datastore.NewKey(pcDownloader.ctx, "Status", "", id, nil)
	err := datastore.Get(pcDownloader.ctx, statusKey, &statusRecord)
	if err != nil {
		log.Criticalf(pcDownloader.ctx, "error pulling status %s: %s\n", params.ByName("id"), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if statusRecord.Error != "" {
		http.Error(w, statusRecord.Error, http.StatusInternalServerError)
		return
	} else if !statusRecord.Done {
		w.WriteHeader(http.StatusNoContent)
		return
	} else {
		err := datastore.Delete(pcDownloader.ctx, statusKey)
		if err != nil {
			log.Criticalf(pcDownloader.ctx, "error deleting status %s: %s\n", params.ByName("id"), err)
		}
		w.WriteHeader(http.StatusOK)
		return
	}
}

func GetPDF(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	pcDownloader := getSession(w, r)

	id, _ := strconv.ParseInt(params.ByName("id"), 10, 64)

	w.Header().Set("Content-Type", "text/plain")
	//w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=directory-%d.pdf", id))
	//streamPDF(pcDownloader.ctx, fmt.Sprintf("%s/pdfs/directory-%d.pdf", pcDownloader.domain, id), w)

	url := generateSignedURL(pcDownloader.ctx, fmt.Sprintf("%s/pdfs/directory-%d.pdf", pcDownloader.domain, id))
	fmt.Fprint(w, url)
}

func GetConfig(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	pcDownloader := getSession(w, r)

	id, _ := strconv.ParseInt(params.ByName("id"), 10, 64)

	config := Config{}

	if pcDownloader.domain == "458648" {
		sections := make([]Section, 9)
		sections[0].Show = true
		sections[0].Phones = true
		sections[0].PhoneCount = 1
		sections[0].Email = true
		sections[0].Address = true
		sections[0].DateJoined = false
		sections[0].Birthday = false
		sections[0].City = true
		sections[0].State = true
		sections[0].PostalCode = true
		sections[0].Country = true
		sections[0].NewMemberFootnote = false
		sections[0].BaptismFootnote = false
		sections[0].Header = "Hinson Memorial Baptist Church"
		sections[0].ListName = "Directory Test"

		sections[1].Show = false
		sections[1].Phones = true
		sections[1].PhoneCount = 2
		sections[1].Email = true
		sections[1].Address = true
		sections[1].DateJoined = true
		sections[1].Birthday = true
		sections[1].City = true
		sections[1].State = true
		sections[1].PostalCode = true
		sections[1].Country = true
		sections[1].NewMemberFootnote = true
		sections[1].BaptismFootnote = true
		sections[1].Header = "Members In-Area and Unable to Attend"
		sections[1].ListName = "Members In-Area and Unable to Attend"
		sections[1].Children = true

		sections[2].Show = false
		sections[2].Phones = true
		sections[2].PhoneCount = 2
		sections[2].Email = true
		sections[2].Address = true
		sections[2].DateJoined = true
		sections[2].Birthday = true
		sections[2].City = true
		sections[2].State = true
		sections[2].PostalCode = true
		sections[2].Country = true
		sections[2].NewMemberFootnote = true
		sections[2].BaptismFootnote = true
		sections[2].Header = "Members Out of Area"
		sections[2].ListName = "Members Out of Area"

		sections[3].Show = true
		sections[3].Age = true
		sections[3].Birthday = true
		sections[3].Header = "Hinson Children"

		sections[4].Show = false
		sections[4].Phones = true
		sections[4].PhoneCount = 1
		sections[4].Email = true
		sections[4].Address = true
		sections[4].City = true
		sections[4].State = true
		sections[4].PostalCode = true
		sections[4].Country = true
		sections[4].Occupation = true
		sections[4].Header = "Supported Workers--Overseas"
		sections[4].ListName = "Supported Workers--Overseas"

		sections[5].Show = false
		sections[5].Phones = true
		sections[5].PhoneCount = 1
		sections[5].Email = true
		sections[5].Address = true
		sections[5].City = true
		sections[5].State = true
		sections[5].PostalCode = true
		sections[5].Country = true
		sections[5].Occupation = true
		sections[5].Header = "Supported Workers--Domestic"
		sections[5].ListName = "Supported Workers--Domestic"

		sections[6].Show = false
		sections[6].Phones = true
		sections[6].PhoneCount = 1
		sections[6].Email = true
		sections[6].Address = true
		sections[6].City = true
		sections[6].State = true
		sections[6].PostalCode = true
		sections[6].Country = true
		sections[6].JobTitle = true
		sections[6].Employer = true
		sections[6].Header = "Pastors Sent Out from CBC"
		sections[6].ListName = "Pastors Sent Out from CBC"

		sections[7].Show = false
		sections[7].Phones = true
		sections[7].PhoneCount = 1
		sections[7].Email = true
		sections[7].Address = true
		sections[7].City = true
		sections[7].State = true
		sections[7].PostalCode = true
		sections[7].Country = true
		sections[7].School = true
		sections[7].Header = "CBC Seminary Report"
		sections[7].ListName = "CBC Seminary Report"

		sections[8].Show = true
		sections[8].Header = "Membership by First Name"
		sections[8].Columns = 3

		config = Config{
			Id:               id,
			PageSize:         "Letter",
			FontFamily:       "Arial",
			TopMargin:        6,
			BottomMargin:     6,
			LeftMargin:       4,
			RightMargin:      4,
			Padding:          8,
			ImagePadding:     4,
			NumberOfColumns:  3,
			ColumnHeight:     22,
			LineHeight:       3.0,
			FontSize:         7.0,
			HighlightOpacity: 0.06,
			Gutter:           4,
			Sections:         sections,
		}
	}

	configRecord := ConfigRecord{}
	configKey := datastore.NewKey(pcDownloader.ctx, "Config", "", id, nil)
	err := datastore.Get(pcDownloader.ctx, configKey, &configRecord)
	if err != nil {
		log.Warningf(pcDownloader.ctx, "error pulling config %s: %s\n", params.ByName("id"), err)
	}
	if configRecord.Config != nil {
		err := json.Unmarshal(configRecord.Config, &config)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(config)
}

func init() {
	sessionStore.SetMaxAge(30 * 24 * 3600)
	router := httprouter.New()

	router.GET("/", Index)
	router.POST("/", Index)

	router.GET("/api/v1/authorize", Authorize)

	router.GET("/api/v1/configs/:id", GetConfig)
	router.POST("/api/v1/configs/:id", SaveConfig)

	router.POST("/api/v1/pdf", CreatePDF)
	router.GET("/api/v1/status/:id", CheckPDF)
	router.GET("/api/v1/pdf/:id", GetPDF)

	router.POST("/api/v1/overrides", SaveOverrides)
	router.GET("/api/v1/overrides", GetOverrides)

	router.POST("/api/v1/workers/pdf", PDFWorker)

	http.Handle("/", router)
}
