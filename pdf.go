package pc_pdf_generator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/file"
	"google.golang.org/appengine/log"

	"github.com/jung-kurt/gofpdf"
	"github.com/nfnt/resize"
	"github.com/pariz/gountries"
	"golang.org/x/net/context"
)

func streamPDF(ctx context.Context, fileName string, w io.Writer) (err error) {
	bucketName, err := file.DefaultBucketName(ctx)
	if err != nil {
		return err
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)

	rc, err := bucket.Object(fileName).NewReader(ctx)

	// chunkSize := int64(31 * 1024 * 1024)
	// buffer := []byte{}

	// for offset := int64(0); true; offset += chunkSize {
	// 	rc, err := bucket.Object(fileName).NewRangeReader(ctx, offset, chunkSize)
	// 	if err != nil {
	// 		if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == 416 {
	// 			// Assume "can't satisfy range" means we read everything.
	// 			break
	// 		}
	// 		return err
	// 	}
	// 	defer rc.Close()
	// 	bytes, err := ioutil.ReadAll(rc)
	// 	buffer = append(buffer, bytes...)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	// rc := bytes.NewReader(buffer)

	if err != nil {
		return err
	}
	defer rc.Close()

	_, err = io.Copy(w, rc)

	if err != nil {
		return err
	}

	bucket.Object(fileName).Delete(ctx)

	return err
}

func generatePDF(config *Config, pcDl PCDownloader, fileId string) (err error) {
	translate, err := gofpdf.UnicodeTranslatorFromFile("iso-8859-1.map")
	if err != nil {
		return err
	}

	fileName := fmt.Sprintf("%s/pdfs/directory-%s.pdf", pcDl.domain, fileId)

	pdfDir := PdfDir{
		topMargin:        config.TopMargin,
		leftMargin:       config.LeftMargin,
		bottomMargin:     config.BottomMargin,
		rightMargin:      config.RightMargin,
		pageSize:         config.PageSize,
		colNum:           config.NumberOfColumns,
		padding:          config.Padding,
		gutter:           config.Gutter,
		imagePadding:     config.ImagePadding,
		columnHeight:     config.ColumnHeight,
		fontFamily:       config.FontFamily,
		fontSize:         config.FontSize,
		lineHeight:       config.LineHeight,
		highlightOpacity: config.HighlightOpacity,
		translate:        translate,
		fileName:         fileName,
		ctx:              pcDl.ctx,
		domain:           pcDl.domain,
		firstNameColumns: 6.0,
	}

	err = pdfDir.setupPDF()
	if err != nil {
		return err
	}

	var members map[string]Household
	if config.Sections[0].Show || len(config.Sections) > 3 && config.Sections[3].Show || len(config.Sections) > 8 && config.Sections[8].Show {
		members, err = pcDl.downloadList(config.Sections[0].ListName)
		if err != nil {
			return err
		}
	}

	if config.Sections[0].Show {
		//DisplayOptions{PhoneCount: 2, ExcludeDirSections: []string{"463631", "463630"}}
		pdfDir.writeSection(members, config.Sections[0].Header, config.Sections[0])
	}

	if len(config.Sections) > 1 && config.Sections[1].Show {
		membersInAreaUnable, err := pcDl.downloadList(config.Sections[1].ListName)
		if err != nil {
			return err
		}

		pdfDir.writeSection(membersInAreaUnable, config.Sections[1].Header, config.Sections[1])
	}

	if len(config.Sections) > 2 && config.Sections[2].Show {
		membersOutArea, err := pcDl.downloadList(config.Sections[2].ListName)
		if err != nil {
			return err
		}

		pdfDir.writeSection(membersOutArea, config.Sections[2].Header, config.Sections[2])
	}

	if len(config.Sections) > 3 && config.Sections[3].Show {
		pdfDir.writeChildren(members, config.Sections[3].Header, config.Sections[3])
	}

	if len(config.Sections) > 4 && config.Sections[4].Show {
		supportedO, err := pcDl.downloadList(config.Sections[4].ListName)
		if err != nil {
			return err
		}

		pdfDir.writeSection(supportedO, config.Sections[4].Header, config.Sections[4])
	}

	if len(config.Sections) > 5 && config.Sections[5].Show {
		supportedD, err := pcDl.downloadList(config.Sections[5].ListName)
		if err != nil {
			return err
		}

		pdfDir.writeSection(supportedD, config.Sections[5].Header, config.Sections[5])
	}

	if len(config.Sections) > 6 && config.Sections[6].Show {
		pastorsSent, err := pcDl.downloadList(config.Sections[6].ListName)
		if err != nil {
			return err
		}

		pdfDir.writeSection(pastorsSent, config.Sections[6].Header, config.Sections[6])
	}

	if len(config.Sections) > 7 && config.Sections[7].Show {
		seminary, err := pcDl.downloadList(config.Sections[7].ListName)
		if err != nil {
			return err
		}

		pdfDir.writeSection(seminary, config.Sections[7].Header, config.Sections[7])
	}

	if len(config.Sections) > 8 && config.Sections[8].Show {
		pdfDir.writeFirstNames(members, config.Sections[8].Header)
	}

	err = pdfDir.closePDF(pcDl.ctx, fileName)
	if err != nil {
		return err
	}

	return err
}

type PdfDir struct {
	leftMargin       float64
	topMargin        float64
	rightMargin      float64
	bottomMargin     float64
	pageSize         string
	colWd            float64
	colNum           float64
	firstNameColumns float64
	padding          float64
	gutter           float64
	imagePadding     float64
	imageWidth       float64
	columnHeight     float64
	fontFamily       string
	fontSize         float64
	lineHeight       float64
	textWidth        float64
	highlightOpacity float64
	overrides        map[string]Section
	ctx              context.Context

	fileName string
	domain   string

	pdf *gofpdf.Fpdf

	translate func(string) string

	gountries *gountries.Query
}

//PDF Generation

func dateDiff(a, b time.Time) (year, month, day, hour, min, sec int) {
	if a.Location() != b.Location() {
		b = b.In(a.Location())
	}
	if a.After(b) {
		a, b = b, a
	}
	y1, M1, d1 := a.Date()
	y2, M2, d2 := b.Date()

	h1, m1, s1 := a.Clock()
	h2, m2, s2 := b.Clock()

	year = int(y2 - y1)
	month = int(M2 - M1)
	day = int(d2 - d1)
	hour = int(h2 - h1)
	min = int(m2 - m1)
	sec = int(s2 - s1)

	// Normalize negative values
	if sec < 0 {
		sec += 60
		min--
	}
	if min < 0 {
		min += 60
		hour--
	}
	if hour < 0 {
		hour += 24
		day--
	}
	if day < 0 {
		// days in month:
		t := time.Date(y1, M1, 32, 0, 0, 0, 0, time.UTC)
		day += 32 - t.Day()
		month--
	}
	if month < 0 {
		month += 12
		year--
	}

	return
}

func (dir *PdfDir) setupPDF() (err error) {
	dir.pdf = gofpdf.New("P", "mm", dir.pageSize, "./fonts")
	dir.pdf.AddFont("Arial Narrow", "", "arial-narrow.json")
	dir.pdf.AddFont("Arial Narrow", "B", "arial-narrow-bold.json")
	dir.pdf.AddFont("Yanone Kaffeesatz", "", "YanoneKaffeesatz-Regular.json")
	dir.pdf.AddFont("Yanone Kaffeesatz", "B", "YanoneKaffeesatz-Bold.json")
	dir.pdf.AddFont("Yanone Kaffeesatz Light", "", "YanoneKaffeesatz-Light.json")
	dir.pdf.AddFont("Yanone Kaffeesatz Thin", "", "YanoneKaffeesatz-Thin.json")

	width, _ := dir.pdf.GetPageSize()

	dir.pdf.SetCellMargin(0)

	dir.colWd = (width - dir.leftMargin - dir.rightMargin - ((dir.colNum - 1) * dir.gutter)) / dir.colNum

	return err
}

func (dir *PdfDir) closePDF(ctx context.Context, fileName string) (err error) {
	bucketName, err := file.DefaultBucketName(ctx)
	if err != nil {
		return err
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)

	wc := bucket.Object(fileName).NewWriter(ctx)
	wc.ContentType = "application/pdf"

	err = dir.pdf.Output(wc)
	if err != nil {
		return err
	}

	err = wc.Close()

	return err
}

func (dir *PdfDir) writeSection(entries map[string]Household, header string, displayOptions Section) (err error) {
	dir.pdf.SetLeftMargin(dir.leftMargin)
	dir.pdf.SetTopMargin(dir.topMargin)
	dir.pdf.SetRightMargin(dir.rightMargin)
	dir.pdf.SetAutoPageBreak(true, dir.bottomMargin)

	dir.pdf.AddPage()
	column := 0.0
	firstPage := true

	dir.pdf.SetFont(dir.fontFamily, "", dir.fontSize+2.0)

	dir.pdf.SetX(dir.leftMargin)
	dir.pdf.SetLeftMargin(dir.leftMargin)
	_, boldLineHeight := dir.pdf.GetFontSize()
	textWidth := dir.pdf.GetStringWidth(dir.translate(header))
	dir.pdf.CellFormat(textWidth, boldLineHeight, dir.translate(header), "", 0, "LC", false, 0, "")
	dir.pdf.SetFont(dir.fontFamily, "", dir.fontSize)

	textWidth = dir.pdf.GetStringWidth(dir.translate(time.Now().Format("As of: 01/02/2006")))
	leftMargin, _, _, _ := dir.pdf.GetMargins()
	_, lineHeight := dir.pdf.GetFontSize()
	width, _ := dir.pdf.GetPageSize()
	dir.pdf.SetLeftMargin(width - textWidth - dir.rightMargin)
	dir.pdf.CellFormat(textWidth, lineHeight, dir.translate(time.Now().Format("As of: 01/02/2006")), "", 0, "RC", false, 0, "")
	dir.pdf.SetLeftMargin(leftMargin)
	dir.pdf.SetY(dir.pdf.GetY() + boldLineHeight*2.0)

	bucketName, err := file.DefaultBucketName(dir.ctx)
	if err != nil {
		return err
	}

	client, err := storage.NewClient(dir.ctx)
	if err != nil {
		return err
	}
	defer client.Close()
	bucket := client.Bucket(bucketName)

	keys := make([]string, len(entries))
	keyToId := make(map[string]string)

	i := 0
	for k := range entries {
		keys[i] = entries[k].SortKey
		keyToId[entries[k].SortKey] = k
		i++
	}
	sort.Strings(keys)

householdLoop:
	for _, k := range keys {
		h := entries[keyToId[k]]

		if h.Head != nil {
			overrideOptions := dir.getSectionOverride(h.Head, displayOptions)
			if !overrideOptions.ShowHousehold {
				continue householdLoop
			}
		}

		//high := h.Head != nil && len(h.Members) > 0
		if h.Head != nil {
			if h.Head.Thumbnail {
				rc, err := bucket.Object(dir.domain + "/jpgs/" + h.Head.Id).NewReader(dir.ctx)
				if err != nil {
					log.Errorf(dir.ctx, "Error loading: %s\n", err)
				}
				defer rc.Close()

				input, _, err := image.Decode(rc)
				if err != nil {
					log.Errorf(dir.ctx, "Head thumbnail decode: %s\n", err)
				}

				ratio := float64(input.Bounds().Max.Y) / dir.columnHeight
				width = float64(input.Bounds().Max.X) / ratio

				resize.Thumbnail(uint(dir.columnHeight), uint(width), input, resize.Lanczos3)
				if err != nil {
					log.Errorf(dir.ctx, "Head thumbnail resize: %s\n", err)
				}

				buf := new(bytes.Buffer)
				err = jpeg.Encode(buf, input, &jpeg.Options{Quality: 75})
				if err != nil {
					log.Errorf(dir.ctx, "Head thumbnail encode: %s\n", err)
				}

				dir.pdf.RegisterImageOptionsReader(h.Head.Id, gofpdf.ImageOptions{ImageType: "JPG"}, buf)
			}
			column, firstPage, _ = dir.writeEntry(*h.Head, column, false, true, firstPage, displayOptions)
		}

	memberLoop:
		for _, m := range h.Members {
			if m.Thumbnail {
				rc, err := bucket.Object(dir.domain + "/jpgs/" + m.Id).NewReader(dir.ctx)
				if err != nil {
					log.Errorf(dir.ctx, "Error loading: %s\n", err)
				}
				defer rc.Close()

				input, _, err := image.Decode(rc)
				if err != nil {
					log.Errorf(dir.ctx, "Member thumbnail decode: %s\n", err)
				}

				ratio := float64(input.Bounds().Max.Y) / dir.columnHeight
				width = float64(input.Bounds().Max.X) / ratio

				resize.Thumbnail(uint(dir.columnHeight), uint(width), input, resize.Lanczos3)
				if err != nil {
					log.Errorf(dir.ctx, "Member thumbnail resize: %s\n", err)
				}

				buf := new(bytes.Buffer)
				err = jpeg.Encode(buf, input, &jpeg.Options{Quality: 75})
				if err != nil {
					log.Errorf(dir.ctx, "Member thumbnail encode: %s\n", err)
				}

				dir.pdf.RegisterImageOptionsReader(m.Id, gofpdf.ImageOptions{ImageType: "JPG"}, buf)
			}
			if cap(displayOptions.ExcludeDirSections) > 0 {
				for _, exclude := range displayOptions.ExcludeDirSections {
					if _, ok := m.DirectorySections[exclude]; ok {
						continue memberLoop
					}
				}
			}
			column, firstPage, _ = dir.writeEntry(*m, column, false, true, firstPage, displayOptions)
		}
	}

	dir.writeFooter(displayOptions)

	return nil
}

func phoneNumber(phone int64) string {
	no := phone % 1e4
	xc := phone / 1e4 % 1e3
	phoneNo := fmt.Sprintf("%03d-%04d", xc, no)
	ac := phone / 1e7 % 1e3
	if ac != 0 {
		phoneNo = fmt.Sprintf("%03d-%s", ac, phoneNo)
	}
	pfx := phone / 1e10
	if pfx != 0 {
		phoneNo = fmt.Sprintf("+%d %s", pfx, phoneNo)
	}

	return phoneNo
}

func (dir *PdfDir) writeFooter(displayOptions Section) {
	_, _, _, bottom := dir.pdf.GetMargins()

	width, height := dir.pdf.GetPageSize()

	_, lineHeight := dir.pdf.GetFontSize()

	dir.pdf.SetAutoPageBreak(false, bottom)

	dir.pdf.SetY(height - lineHeight - bottom)
	dir.pdf.SetX(dir.leftMargin)
	dir.pdf.SetLeftMargin(dir.leftMargin)

	if displayOptions.BaptismFootnote {
		textWidth := dir.pdf.GetStringWidth(dir.translate("§ Member pending baptism"))
		dir.pdf.CellFormat(textWidth, lineHeight, dir.translate("§ Member pending baptism"), "", 0, "LB", false, 0, "")
	}

	if displayOptions.NewMemberFootnote {
		textWidth := dir.pdf.GetStringWidth(dir.translate("* New member in the last 90 days"))
		leftMargin, _, _, _ := dir.pdf.GetMargins()
		dir.pdf.SetLeftMargin(width - textWidth - dir.rightMargin)
		dir.pdf.CellFormat(textWidth, lineHeight, dir.translate("* New member in the last 90 days"), "", 0, "RB", false, 0, "")
		dir.pdf.SetLeftMargin(leftMargin)
	}

	dir.pdf.SetAutoPageBreak(true, bottom)
}

func (dir *PdfDir) shrinkedCell(width float64, height float64, str string, border string, alignment string, fill bool) {
	originalFontSize, _ := dir.pdf.GetFontSize()
	for dir.pdf.GetStringWidth(str)+dir.pdf.GetCellMargin() > width {
		currentFontSize, _ := dir.pdf.GetFontSize()
		dir.pdf.SetFontSize(currentFontSize - 0.1)
	}
	dir.pdf.CellFormat(width, height, str, border, 1, alignment, fill, 0, "")
	dir.pdf.SetFontSize(originalFontSize)
}

func (dir *PdfDir) getSectionOverride(directoryEntry *Person, oldDisplayOptions Section) (displayOptions Section) {
	displayOptions = oldDisplayOptions
	displayOptions.ShowHousehold = true
	displayOptions.ShowChildren = true

	if cap(displayOptions.ExcludeDirSections) > 0 {
		for _, exclude := range displayOptions.ExcludeDirSections {
			if _, ok := directoryEntry.DirectorySections[exclude]; ok {
				displayOptions.Show = false
				return displayOptions
			}
		}
	}

	key := strings.ToLower(fmt.Sprintf("%s-%s-%s", directoryEntry.FirstName, directoryEntry.LastName, directoryEntry.Birthday.Format("2006-01-02")))

	if dir.overrides == nil {
		overrideRecord := OverridesRecord{}
		overrideKey := datastore.NewKey(dir.ctx, "Overrides", "", 1, nil)
		err := datastore.Get(dir.ctx, overrideKey, &overrideRecord)
		if err != nil {
			log.Warningf(dir.ctx, "Error pulling overrides: %s\n", err)
			return displayOptions
		}
		if overrideRecord.Overrides != nil {
			err := json.Unmarshal(overrideRecord.Overrides, &dir.overrides)
			if err != nil {
				log.Warningf(dir.ctx, "Error parsing overrides: %s\n", err)
				return displayOptions
			}
		}
	}

	overrideOptions := dir.overrides[key]

	if overrideOptions.KeyFirstName != "" {

		if displayOptions.Show && !overrideOptions.Show {
			displayOptions.Show = false
		}

		if displayOptions.Phones && !overrideOptions.Phones {
			displayOptions.Phones = false
		}

		if displayOptions.Email && !overrideOptions.Email {
			displayOptions.Email = false
		}

		if displayOptions.Address && !overrideOptions.Address {
			displayOptions.Address = false
		}

		if displayOptions.DateJoined && !overrideOptions.DateJoined {
			displayOptions.DateJoined = false
		}

		if displayOptions.Birthday && !overrideOptions.Birthday {
			displayOptions.Birthday = false
		}

		if displayOptions.City && !overrideOptions.City {
			displayOptions.City = false
		}

		if displayOptions.State && !overrideOptions.State {
			displayOptions.State = false
		}

		if displayOptions.PostalCode && !overrideOptions.PostalCode {
			displayOptions.PostalCode = false
		}

		if displayOptions.Country && !overrideOptions.Country {
			displayOptions.Country = false
		}

		if displayOptions.ShowHousehold && !overrideOptions.ShowHousehold {
			displayOptions.ShowHousehold = false
		}

		if displayOptions.ShowChildren && !overrideOptions.ShowChildren {
			displayOptions.ShowChildren = false
		}
	}
	return displayOptions
}

func (dir *PdfDir) writeEntry(directoryEntry Person, lastColumn float64, highlightTop bool, highlightBottom bool, indentPage bool, displayOptions Section) (column float64, firstPage bool, err error) {
	displayOptions = dir.getSectionOverride(&directoryEntry, displayOptions)
	if !displayOptions.Show {
		return lastColumn, indentPage, nil
	}
	firstPage = indentPage

	left, top, _, bottom := dir.pdf.GetMargins()

	_, height := dir.pdf.GetPageSize()

	startOffset := 0.0
	endOffset := 0.0

	if displayOptions.BaptismFootnote || displayOptions.NewMemberFootnote {
		_, lineHeight := dir.pdf.GetFontSize()
		endOffset = lineHeight
	}

	if firstPage {
		startOffset = ((dir.fontSize + 2.0) / dir.pdf.GetConversionRatio()) * 2.0
	}

	if dir.pdf.GetY()+dir.columnHeight+(dir.padding) > height-bottom-endOffset {
		lastColumn++

		dir.pdf.SetY(top + startOffset)
	}

	if lastColumn >= dir.colNum {
		lastColumn = 0

		dir.pdf.SetLeftMargin(dir.leftMargin)
		dir.pdf.SetX(dir.leftMargin)
		dir.pdf.SetY(height - bottom)

		dir.writeFooter(displayOptions)

		dir.pdf.AddPage()
		dir.pdf.SetY(top)
		dir.pdf.SetX(left)
		dir.pdf.SetLeftMargin(left)
		firstPage = false
	}

	halfPadding := dir.padding / 2.0
	dir.pdf.SetY(dir.pdf.GetY() + halfPadding)
	startY := dir.pdf.GetY()

	x := dir.leftMargin + float64(lastColumn)*(dir.colWd+dir.gutter)

	dir.pdf.SetLeftMargin(x)
	dir.pdf.SetX(x)

	color := int(math.Ceil(255 - (dir.highlightOpacity * 255)))

	if highlightTop {
		dir.pdf.SetFillColor(color, color, color)
		dir.pdf.SetDrawColor(color, color, color)
		fillHeight := dir.columnHeight + halfPadding + (halfPadding / 2)
		diff := (height - dir.bottomMargin) - (dir.pdf.GetY() + fillHeight)
		if dir.pdf.GetY()+fillHeight > height-dir.bottomMargin {
			fillHeight = fillHeight + diff
		}
		dir.pdf.Rect(dir.pdf.GetX(), dir.pdf.GetY()-(halfPadding/2), dir.colWd, fillHeight, "FD")
	}

	if highlightBottom {
		dir.pdf.SetFillColor(color, color, color)
		dir.pdf.SetDrawColor(color, color, color)
		startY := dir.pdf.GetY() - halfPadding
		fillHeight := dir.columnHeight + halfPadding + (halfPadding / 2)
		if startY < dir.topMargin {
			fillHeight = fillHeight - (dir.topMargin - startY)
			startY = dir.topMargin
		}
		dir.pdf.Rect(dir.pdf.GetX(), startY, dir.colWd, fillHeight, "FD")
	}

	dir.pdf.SetTextColor(0, 0, 0)
	if directoryEntry.Thumbnail {
		//dir.pdf.Image(directoryEntry.Id, dir.pdf.GetX()+dir.imagePadding, dir.pdf.GetY(), 0, dir.columnHeight, false, "", 0, "")
		dir.pdf.Image(directoryEntry.Id, dir.pdf.GetX()+dir.imagePadding, dir.pdf.GetY(), 25, 0, false, "", 0, "")
		if dir.pdf.GetImageInfo(directoryEntry.Id) != nil {
			ratio := dir.pdf.GetImageInfo(directoryEntry.Id).Height() / dir.columnHeight
			dir.imageWidth = dir.pdf.GetImageInfo(directoryEntry.Id).Width() / ratio
		}
	}

	dir.imageWidth = 25
	dir.pdf.SetLeftMargin(x + dir.imageWidth + (dir.imagePadding * 2))

	prefix := ""
	if directoryEntry.NewMember90 && displayOptions.NewMemberFootnote {
		prefix = "*"
	}

	if directoryEntry.PendingBaptism && displayOptions.BaptismFootnote {
		prefix = "§"
	}

	dir.textWidth = dir.colWd - (dir.imageWidth + (dir.imagePadding * 2))

	text := fmt.Sprintf("%s, %s%s", strings.ToUpper(directoryEntry.LastName), strings.ToUpper(directoryEntry.FirstName), prefix)
	dir.pdf.SetFont(dir.fontFamily, "B", dir.fontSize)
	dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate(text), "", "L", false)

	dir.pdf.SetFont(dir.fontFamily, "", dir.fontSize)

	if displayOptions.Occupation && directoryEntry.Occupation != "" {
		dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate(directoryEntry.Occupation), "", "L", false)
	}

	if displayOptions.JobTitle && directoryEntry.Title != "" {
		dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate(directoryEntry.Title), "", "L", false)
	}

	if displayOptions.Employer && directoryEntry.Employer != "" {
		dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate(directoryEntry.Employer), "", "L", false)
	}

	if displayOptions.School && directoryEntry.School != "" {
		dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate(directoryEntry.School), "", "L", false)
	}

	if displayOptions.Address && directoryEntry.Address1 != "" {
		dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate(directoryEntry.Address1), "", "L", false)
	}

	if displayOptions.Address && directoryEntry.Address2 != "" {
		dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate(directoryEntry.Address2), "", "L", false)
	}

	if directoryEntry.City != "" || directoryEntry.State != "" || directoryEntry.PostalCode != "" {
		addressText := ""
		if displayOptions.City && directoryEntry.City != "" {
			addressText = addressText + directoryEntry.City + ", "
		}

		if directoryEntry.Country == "US" || directoryEntry.Country == "CA" {
			if displayOptions.State && directoryEntry.State != "" {
				addressText = addressText + directoryEntry.State + " "
			}
			if displayOptions.PostalCode && directoryEntry.PostalCode != "" {
				postalCode := strings.Split(directoryEntry.PostalCode, "-")[0]
				addressText = addressText + postalCode + " "
			}
		} else {
			if dir.gountries == nil {
				dir.gountries = gountries.New()
			}

			if displayOptions.Country && directoryEntry.Country != "" {
				country, _ := dir.gountries.FindCountryByAlpha(directoryEntry.Country)
				addressText = addressText + country.Name.Common + " "
			}
		}
		if addressText != "" {
			dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate(addressText), "", "L", false)
		}
	}

	if displayOptions.Email && directoryEntry.EmailAddress != "" {
		dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate(directoryEntry.EmailAddress), "", "L", false)
	}

	if displayOptions.Phones {
		phones := 0

		if directoryEntry.CellPhone != 0 {
			phones++
			dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate("C: "+phoneNumber(directoryEntry.CellPhone)), "", "L", false)
		}

		if directoryEntry.HomePhone != 0 && phones < displayOptions.PhoneCount {
			phones++
			dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate("H: "+phoneNumber(directoryEntry.HomePhone)), "", "L", false)
		}

		if directoryEntry.WorkPhone != 0 && phones < displayOptions.PhoneCount {
			dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate("W: "+phoneNumber(directoryEntry.WorkPhone)), "", "L", false)
		}
	}

	if displayOptions.DateJoined || displayOptions.Birthday {
		dateText := ""

		if displayOptions.DateJoined {
			dateText = fmt.Sprintf("DJ: %s", directoryEntry.DateJoined.Format("01/2006"))
		}

		if displayOptions.Birthday {
			dateText = dateText + fmt.Sprintf(" BD: %s", directoryEntry.Birthday.Format("01/02"))
		}

		dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate(dateText), "", "L", false)
	}

	if displayOptions.Children && directoryEntry.Children1 != "" {
		dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate(directoryEntry.Children1), "", "L", false)
	}

	if displayOptions.Children && directoryEntry.Children2 != "" {
		dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate(directoryEntry.Children2), "", "L", false)
	}

	dir.pdf.SetY(startY + dir.columnHeight + halfPadding)

	column = lastColumn

	return column, firstPage, nil
}

func (dir *PdfDir) writeFirstNames(entries map[string]Household, header string) (err error) {
	dir.pdf.SetLeftMargin(dir.leftMargin)
	dir.pdf.SetTopMargin(dir.topMargin)
	dir.pdf.SetRightMargin(dir.rightMargin)
	dir.pdf.SetAutoPageBreak(true, dir.bottomMargin)

	dir.pdf.AddPage()

	dir.pdf.SetFont(dir.fontFamily, "", dir.fontSize+2.0)

	dir.pdf.SetX(dir.leftMargin)
	dir.pdf.SetLeftMargin(dir.leftMargin)

	dir.pdf.SetFont(dir.fontFamily, "", dir.fontSize+2.0)

	dir.pdf.SetX(dir.leftMargin)
	dir.pdf.SetLeftMargin(dir.leftMargin)
	_, boldLineHeight := dir.pdf.GetFontSize()
	textWidth := dir.pdf.GetStringWidth(dir.translate(header))
	dir.pdf.CellFormat(textWidth, boldLineHeight, dir.translate(header), "", 0, "LC", false, 0, "")
	dir.pdf.SetFont(dir.fontFamily, "", dir.fontSize)

	textWidth = dir.pdf.GetStringWidth(dir.translate(time.Now().Format("As of: 01/02/2006")))
	leftMargin, _, _, _ := dir.pdf.GetMargins()
	_, lineHeight := dir.pdf.GetFontSize()
	width, _ := dir.pdf.GetPageSize()
	dir.pdf.SetLeftMargin(width - textWidth - dir.rightMargin)
	dir.pdf.CellFormat(textWidth, lineHeight, dir.translate(time.Now().Format("As of: 01/02/2006")), "", 0, "RC", false, 0, "")
	dir.pdf.SetLeftMargin(leftMargin)
	dir.pdf.SetY(dir.pdf.GetY() + boldLineHeight*2.0)

	entriesKeyed := make(map[string]*Person)
	keys := make([]string, 0)

householdLoop:
	for _, h := range entries {
		if h.Head != nil {
			overrideOptions := dir.getSectionOverride(h.Head, Section{Show: true})
			if !overrideOptions.ShowHousehold {
				continue householdLoop
			}
		}

		if h.Head != nil {
			key := h.Head.FirstName + h.Head.LastName
			entriesKeyed[key] = h.Head
			keys = append(keys, key)
		}

		for _, m := range h.Members {
			key := m.FirstName + m.LastName
			entriesKeyed[key] = m
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)

	i := 0
	count := 6.0
	lastColumn := 0.0
	firstPage := true
	left, top, _, bottom := dir.pdf.GetMargins()

	width, height := dir.pdf.GetPageSize()

	colWd := (width - dir.leftMargin - dir.rightMargin) / count

memberLoop:
	for _, k := range keys {
		p := entriesKeyed[k]
		overrideOptions := dir.getSectionOverride(p, Section{Show: true})
		if !overrideOptions.Show {
			continue memberLoop
		}

		if dir.pdf.GetY()+dir.lineHeight > height-bottom {
			lastColumn++
			startOffset := 0.0

			if firstPage {
				startOffset = dir.fontSize + 2.0
			}

			dir.pdf.SetY(top + startOffset)
		}

		if lastColumn >= dir.firstNameColumns {
			lastColumn = 0.0

			dir.pdf.SetLeftMargin(dir.leftMargin)
			dir.pdf.SetX(dir.leftMargin)
			dir.pdf.SetY(height - bottom)

			dir.pdf.AddPage()
			dir.pdf.SetY(top)
			dir.pdf.SetX(left)
			dir.pdf.SetLeftMargin(left)
			firstPage = false
		}

		x := dir.leftMargin + float64(lastColumn)*colWd

		dir.pdf.SetLeftMargin(x)
		dir.pdf.SetX(x)

		dir.shrinkedCell(dir.textWidth, dir.lineHeight, dir.translate(p.FirstName+" "+p.LastName), "", "L", false)
		i++
	}

	return nil
}

func (dir *PdfDir) writeChildrenHeader(leftSide, colWd, offset float64, displayOptions Section) {
	dir.pdf.SetLeftMargin(leftSide)
	dir.pdf.SetX(leftSide)

	dir.pdf.Line(leftSide, dir.pdf.GetY(), leftSide+colWd-offset-2.0, dir.pdf.GetY())
	dir.pdf.SetY(dir.pdf.GetY() + dir.pdf.GetLineWidth() + displayOptions.LineSpacing)

	dir.pdf.SetFont(dir.fontFamily, "B", dir.fontSize)
	_, boldLineHeight := dir.pdf.GetFontSize()
	lineHeight := boldLineHeight

	textWidth := dir.pdf.GetStringWidth(dir.translate("Parents/Children"))
	dir.pdf.CellFormat(textWidth, lineHeight, dir.translate("Parents/Children"), "", 0, "LC", false, 0, "")

	if displayOptions.Age && displayOptions.Birthday {
		dir.pdf.SetLeftMargin(leftSide + 60.0)
		dir.pdf.SetX(leftSide + 60.0)
		textWidth := dir.pdf.GetStringWidth(dir.translate("Age"))
		dir.pdf.CellFormat(textWidth, lineHeight, dir.translate("Age"), "", 0, "LC", false, 0, "")

		dir.pdf.SetLeftMargin(leftSide)
		dir.pdf.SetX(leftSide)

		dir.pdf.CellFormat(colWd-offset-2.0, lineHeight, dir.translate("Birthday"), "", 0, "RC", false, 0, "")
	} else if !displayOptions.Age && displayOptions.Birthday {
		dir.pdf.SetLeftMargin(leftSide)
		dir.pdf.SetX(leftSide)

		dir.pdf.CellFormat(colWd-offset-2.0, lineHeight, dir.translate("Birthday"), "", 0, "RC", false, 0, "")
	} else if displayOptions.Age && !displayOptions.Birthday {
		dir.pdf.SetLeftMargin(leftSide)
		dir.pdf.SetX(leftSide)

		dir.pdf.CellFormat(colWd-offset-2.0, lineHeight, dir.translate("Age"), "", 0, "RC", false, 0, "")
	}

	dir.pdf.SetY(dir.pdf.GetY() + lineHeight)
}

func (dir *PdfDir) writeChildren(entries map[string]Household, header string, displayOptions Section) (err error) {
	dir.pdf.SetLeftMargin(dir.leftMargin)
	dir.pdf.SetTopMargin(dir.topMargin)
	dir.pdf.SetRightMargin(dir.rightMargin)
	dir.pdf.SetAutoPageBreak(true, dir.bottomMargin)

	dir.pdf.AddPage()
	column := 0.0
	leftOffset := 5.0
	offset := dir.fontSize
	firstPage := true
	maxColumns := 2.0

	if !displayOptions.Age && !displayOptions.Birthday {
		maxColumns = 3.0
	}

	dir.pdf.SetFont(dir.fontFamily, "", dir.fontSize+2.0)

	dir.pdf.SetX(dir.leftMargin)
	dir.pdf.SetLeftMargin(dir.leftMargin)

	dir.pdf.SetFont(dir.fontFamily, "", dir.fontSize+2.0)

	dir.pdf.SetX(dir.leftMargin)
	dir.pdf.SetLeftMargin(dir.leftMargin)
	_, boldLineHeight := dir.pdf.GetFontSize()
	textWidth := dir.pdf.GetStringWidth(dir.translate(header))
	dir.pdf.CellFormat(textWidth, boldLineHeight, dir.translate(header), "", 0, "LC", false, 0, "")
	dir.pdf.SetFont(dir.fontFamily, "", dir.fontSize)

	textWidth = dir.pdf.GetStringWidth(dir.translate(time.Now().Format("As of: 01/02/2006")))
	leftMargin, _, _, _ := dir.pdf.GetMargins()
	_, lineHeight := dir.pdf.GetFontSize()
	width, _ := dir.pdf.GetPageSize()
	dir.pdf.SetLeftMargin(width - textWidth - dir.rightMargin)
	dir.pdf.CellFormat(textWidth, lineHeight, dir.translate(time.Now().Format("As of: 01/02/2006")), "", 0, "RC", false, 0, "")
	dir.pdf.SetLeftMargin(leftMargin)
	dir.pdf.SetY(dir.topMargin + offset)

	width, height := dir.pdf.GetPageSize()

	dir.pdf.SetDrawColor(0, 0, 0)

	colWd := (width - dir.leftMargin - dir.rightMargin) / maxColumns

	leftSide := dir.leftMargin + (colWd * column)
	dir.writeChildrenHeader(leftSide, colWd, leftOffset, displayOptions)

	dir.pdf.SetLeftMargin(dir.leftMargin)
	dir.pdf.SetX(dir.leftMargin)

	keys := make([]string, len(entries))
	keyToId := make(map[string]string)

	i := 0
	for k := range entries {
		keys[i] = entries[k].SortKey
		keyToId[entries[k].SortKey] = k
		i++
	}
	sort.Strings(keys)

householdLoop:
	for _, k := range keys {
		h := entries[keyToId[k]]

		if h.Head != nil {
			overrideOptions := dir.getSectionOverride(h.Head, displayOptions)
			if !overrideOptions.ShowHousehold || !overrideOptions.ShowChildren {
				continue householdLoop
			}
		}

		if h.Head != nil && cap(displayOptions.ExcludeDirSections) > 0 {
			for _, exclude := range displayOptions.ExcludeDirSections {
				if _, ok := h.Head.DirectorySections[exclude]; ok {
					continue householdLoop
				}
			}
		}

		if len(h.Children) < 1 {
			continue householdLoop
		}

		leftSide := 0.0
		rightSide := 0.0

		_, lineHeight := dir.pdf.GetFontSize()
		dir.pdf.SetFont(dir.fontFamily, "B", dir.fontSize)
		_, boldLineHeight := dir.pdf.GetFontSize()
		dir.pdf.SetFont(dir.fontFamily, "", dir.fontSize)

		entryHeight := displayOptions.LineSpacing + dir.pdf.GetLineWidth() + displayOptions.LineSpacing + boldLineHeight + displayOptions.LineSpacing + (float64(len(h.Children)) * (lineHeight + displayOptions.LineSpacing))
		if dir.pdf.GetY()+entryHeight > height-dir.bottomMargin {
			if column >= maxColumns-1 {
				column = 0
				firstPage = false
				offset = 0
				dir.pdf.AddPage()
				dir.pdf.Line(colWd, dir.pdf.GetY(), colWd, height-dir.bottomMargin)
			} else {
				column++
				dir.pdf.Line(colWd*column, dir.topMargin+offset, colWd*column, height-dir.bottomMargin)
			}

			if column == maxColumns-1 {
				leftOffset = 0.0
			} else {
				leftOffset = 5.0
			}

			dir.pdf.SetY(dir.topMargin + offset)

			if column == 0 {
				leftOffset = 5.0
				leftSide = dir.leftMargin + (colWd * column)
				rightSide = leftSide + colWd - leftOffset - 2.0
			} else if column != maxColumns-1 {
				leftOffset = 4.0
				leftSide = dir.leftMargin + (colWd * column) - 1.0
				rightSide = leftSide + colWd - leftOffset - 1.0
			} else {
				leftOffset = 0.0
				leftSide = dir.leftMargin + (colWd * column) - 1.0
				rightSide = leftSide + colWd - leftOffset - 2.0
			}

			if firstPage {
				dir.writeChildrenHeader(leftSide, colWd, leftOffset, displayOptions)
			}
		}

		if column == 0 {
			leftOffset = 5.0
			leftSide = dir.leftMargin + (colWd * column)
			rightSide = leftSide + colWd - leftOffset - 2.0
		} else if column != maxColumns-1 {
			leftOffset = 5.0
			leftSide = dir.leftMargin + (colWd * column) - 1.0
			rightSide = leftSide + colWd - leftOffset - 1.0
		} else {
			leftOffset = 0.0
			leftSide = dir.leftMargin + (colWd * column) - 1.0
			rightSide = leftSide + colWd - leftOffset - 2.0
		}

		overrideOptions := Section{Show: true}
		if h.Head != nil {
			overrideOptions = dir.getSectionOverride(h.Head, displayOptions)
		}

		str := ""
		if h.Head != nil && overrideOptions.Show {
			str = fmt.Sprintf("%s, %s", h.Head.LastName, h.Head.FirstName)
		}

		for _, m := range h.Members {
			if h.Head != nil && m.LastName != h.Head.LastName && overrideOptions.Show {
				str = fmt.Sprintf("%s and %s %s", str, m.FirstName, m.LastName)
			} else if str != "" {
				str = fmt.Sprintf("%s and %s", str, m.FirstName)
			} else {
				str = fmt.Sprintf("%s, %s", m.LastName, m.FirstName)
			}
		}

		_, originalFontHeight := dir.pdf.GetFontSize()

		dir.pdf.SetX(leftSide)
		dir.pdf.SetLeftMargin(leftSide)
		dir.pdf.SetY(dir.pdf.GetY() + displayOptions.LineSpacing)
		dir.pdf.Line(leftSide, dir.pdf.GetY(), rightSide, dir.pdf.GetY())
		dir.pdf.SetY(dir.pdf.GetY() + displayOptions.LineSpacing)

		dir.pdf.SetFont(dir.fontFamily, "B", dir.fontSize)
		dir.pdf.Write(dir.lineHeight, dir.translate(str))
		dir.pdf.SetFont(dir.fontFamily, "", dir.fontSize)
		dir.pdf.SetY(dir.pdf.GetY() + originalFontHeight + displayOptions.LineSpacing)

		var cKeys sort.StringSlice
		cKeys = make([]string, len(h.Children))
		cKeyToId := make(map[string]string)

		i := 0
		for k := range h.Children {
			c := h.Children[k]
			key := c.Birthday.Format("2006-01-02") + c.FirstName
			cKeys[i] = key
			cKeyToId[key] = k
			i++
		}
		sort.Strings(cKeys)
		sort.Reverse(cKeys)

		for _, k := range cKeys {
			c := h.Children[cKeyToId[k]]

			dir.pdf.SetLeftMargin(leftSide + 4.0)
			dir.pdf.SetX(leftSide + 4.0)
			dir.pdf.Write(dir.lineHeight, dir.translate(c.FirstName))

			years, months, days, _, _, _ := dateDiff(c.Birthday, time.Now())
			text := ""
			if months == 0 && years == 0 {
				text = fmt.Sprintf("%d days", days)
			} else if years == 0 {
				text = fmt.Sprintf("%d mos.", months)
			} else {
				text = fmt.Sprintf("%d", years)
			}

			if displayOptions.Age && displayOptions.Birthday {
				dir.pdf.SetLeftMargin(leftSide + 60.0)
				dir.pdf.SetX(leftSide + 60.0)
				dir.pdf.Write(dir.lineHeight, dir.translate(text))

				dir.pdf.SetLeftMargin(leftSide)
				dir.pdf.SetX(leftSide)

				lineWidth := dir.pdf.GetStringWidth(dir.translate(c.Birthday.Format("Jan 02, 2006")))
				lMargin, _, rMargin, _ := dir.pdf.GetMargins()
				dir.pdf.SetLeftMargin(lMargin + ((colWd - leftOffset) - lineWidth) - rMargin)
				dir.pdf.Write(dir.lineHeight, dir.translate(c.Birthday.Format("Jan 02, 2006")))
				dir.pdf.SetLeftMargin(lMargin)

				dir.pdf.SetY(dir.pdf.GetY() + originalFontHeight + displayOptions.LineSpacing)
			} else if displayOptions.Age && !displayOptions.Birthday {
				dir.pdf.SetLeftMargin(leftSide)
				dir.pdf.SetX(leftSide)

				lineWidth := dir.pdf.GetStringWidth(dir.translate(text))
				lMargin, _, rMargin, _ := dir.pdf.GetMargins()
				dir.pdf.SetLeftMargin(lMargin + ((colWd - leftOffset) - lineWidth) - rMargin)
				dir.pdf.Write(dir.lineHeight, dir.translate(text))
				dir.pdf.SetLeftMargin(lMargin)

				dir.pdf.SetY(dir.pdf.GetY() + originalFontHeight + displayOptions.LineSpacing)
			} else if !displayOptions.Age && displayOptions.Birthday {
				dir.pdf.SetLeftMargin(leftSide)
				dir.pdf.SetX(leftSide)

				lineWidth := dir.pdf.GetStringWidth(dir.translate(c.Birthday.Format("Jan 02, 2006")))
				lMargin, _, rMargin, _ := dir.pdf.GetMargins()
				dir.pdf.SetLeftMargin(lMargin + ((colWd - leftOffset) - lineWidth) - rMargin)
				dir.pdf.Write(dir.lineHeight, dir.translate(c.Birthday.Format("Jan 02, 2006")))
				dir.pdf.SetLeftMargin(lMargin)

				dir.pdf.SetY(dir.pdf.GetY() + originalFontHeight + displayOptions.LineSpacing)
			} else {
				dir.pdf.SetY(dir.pdf.GetY() + originalFontHeight + displayOptions.LineSpacing)
			}
		}
		dir.pdf.SetY(dir.pdf.GetY() - displayOptions.LineSpacing)
	}

	return nil
}
