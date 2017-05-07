package main

import (
	"fmt"
	"time"
	"strings"
	"encoding/json"
	"strconv"
	"regexp"
	"io/ioutil"
	"text/template"
	"bytes"

	"github.com/parnurzeal/gorequest"
	"gopkg.in/alecthomas/kingpin.v2"
	log "github.com/Sirupsen/logrus"
)

const (
	ver string = "0.16"
)

var (
	esURL = kingpin.Flag("url", "elasticsearch URL").Default("http://localhost:9200").Short('u').String()
	timeout = kingpin.Flag("timeout", "timeout for HTTP requests in seconds").Default("10").Short('t').Int()
	shardLimit = kingpin.Flag("shard-limit", "max shard size in GB").Default("32").Short('s').Int()
	defaultShardNumber = kingpin.Flag("default-shard-number", "default number of shards").Default("1").Short('d').Int()
	templateFilePath = kingpin.Flag("template-file", "path to template file").Default("template.json.tmpl").Short('m').String()
	maxDeltaThreshold = kingpin.Flag("max-delta-threshold", "max percentage difference in shard number while decreasing").Default("15").Int()
	dryRun = kingpin.Flag("dry-run", "dry run").Short('n').Bool()
)

// Shard : struct containts shard data
type Shard struct {
	Index string `json:"index"`
	Shard string `json:"shard"`
	PriRep string `json:"prirep"`
	State string `json:"state"`
	Docs string `json:"docs"`
	Store string `json:"store"`
	IP string `json:"ip"`
	Node string `json:"node"`
}

// Template : struct containts template data
type Template struct {
	Settings struct {
		Index struct {
			NumberOfShards string `json:"number_of_shards"`
		} `json:"index"`
	} `json:"settings"`
}

// TemplateJSON : struct containts JSON template data
type TemplateJSON struct {
	NumberOfShards string
	TemplatePattern string
	TemplateOrder int
}

func esQueryGet(url string, timeout int) (string, error) {
	request := gorequest.New()
	resp, body, errs := request.Get(url).Timeout(time.Duration(timeout) * time.Second).End()

	if errs != nil {
		var errsStr []string
		for _, e := range errs {
			errsStr = append(errsStr, fmt.Sprintf("%s", e))
		}
		return "", fmt.Errorf("%s", strings.Join(errsStr, ", "))
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP response code: %s", resp.Status)
	}
	return body, nil
}

func esQueryPut(url string, timeout int, content string) (string, error) {
	request := gorequest.New()
	resp, body, errs := request.Put(url).Send(content).Timeout(time.Duration(timeout) * time.Second).End()

	if errs != nil {
		var errsStr []string
		for _, e := range errs {
			errsStr = append(errsStr, fmt.Sprintf("%s", e))
		}
		return "", fmt.Errorf("%s", strings.Join(errsStr, ", "))
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP response code: %s", resp.Status)
	}
	return body, nil
}

func esQueryDelete(url string, timeout int) (string, error) {
	request := gorequest.New()
	resp, body, errs := request.Delete(url).Timeout(time.Duration(timeout) * time.Second).End()

	if errs != nil {
		var errsStr []string
		for _, e := range errs {
			errsStr = append(errsStr, fmt.Sprintf("%s", e))
		}
		return "", fmt.Errorf("%s", strings.Join(errsStr, ", "))
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP response code: %s", resp.Status)
	}
	return body, nil
}

func parseShards(data string) ([]Shard, error) {
	var shards []Shard
	err := json.Unmarshal([]byte(data), &shards)
	if err != nil {
		return shards, fmt.Errorf("JSON parse failed")
	}
	return shards, nil
}

func parseTemplate(data string) (map[string]Template, error) {
	template := map[string]Template{}

	err := json.Unmarshal([]byte(data), &template)
	if err != nil {
		return template, fmt.Errorf("JSON parse failed")
	}
	return template, nil
}

func sumIndexShardSize(shards []Shard) map[string]map[string]int {
	indexes := make(map[string]map[string]int)
	for _, shard := range shards {
		if shard.State == "STARTED" && shard.PriRep == "p" {
			i, err := strconv.Atoi(shard.Store)
			if err != nil {
				log.Errorf("cannot convert string to int: %s (shard info: %s)", err, shard)
				continue
			}

			if _, ok := indexes[shard.Index]; ok {
				indexes[shard.Index]["size"] += i
				indexes[shard.Index]["number"]++
			} else {
				indexes[shard.Index] = map[string]int{
					"size": i,
					"number": 1,
				}
			}
		}
	}

	return indexes
}

func calculateNumerOfShards(shards []Shard, templates map[string]Template, shardLimit int) (map[string]map[string]interface{}, error) {
	shardLimit = shardLimit * 1024 * 1024 * 1024
	indexes := sumIndexShardSize(shards)

	results := make(map[string]map[string]interface{})
	re := regexp.MustCompile(`^(logstash-\S+)-\d{4}\.\d{2}\.\d{2}$`)
	for k, v := range indexes {
		var pattern string
		if matches := re.FindStringSubmatch(k); matches != nil {
			pattern = matches[1]
		} else {
			log.Warnf("cannot find pattern in index %s", k)
			continue
		}

		num, err := getTemplateNumberOfShareds(templates, pattern)
		if err == nil {
			results[k] = map[string]interface{}{
				"template_number_of_shards": num,
			}
		} else {
			results[k] = map[string]interface{}{
				"template_number_of_shards": 0,
			}
		}
		results[k]["target_number_of_shards"] = v["size"] / shardLimit + 1
		results[k]["template_pattern"] = pattern
	}

	return results, nil
}

func getTemplateNumberOfShareds(templates map[string]Template, templateName string) (int, error) {
	i, err := strconv.Atoi(templates[templateName].Settings.Index.NumberOfShards)
	if err != nil {
		return 0, err
	}

	return i, nil
}

func getTemplates(esURL string, timeout int) (map[string]Template, error) {
	url := esURL + "/_template"

	esData, err := esQueryGet(url, timeout)
	if err != nil {
		return map[string]Template{}, err
	}

	templateData, err := parseTemplate(esData)
	if err != nil {
		return map[string]Template{}, err
	}

	return templateData, nil
}

func getShards(esURL string, timeout int) ([]Shard, error) {
	currentTime := time.Now().Local()
	url := esURL + "/_cat/shards/logstash*" + currentTime.Format("2006.01.02") + "?format=json&bytes=b"

	esData, err := esQueryGet(url, timeout)
	if err != nil {
		return []Shard{}, err
	}

	shards, err := parseShards(esData)
	if err != nil {
		return []Shard{}, err
	}

	return shards, nil
}

func readTemplateFile(filePath string) (string, error) {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Errorf("cannot read template file %s: %s", filePath, err)
		return "", err
	}
	return string(b), nil
}

func getRenderedTemplate(templateSource, numberOfShards, templatePattern string, templateOrder int) string {
	t := TemplateJSON{
		numberOfShards,
		templatePattern + "-*",
		templateOrder,
	}

	tmpl, err := template.New("TemplateJSON").Parse(templateSource)
	if err != nil {
		panic(err)
	}

	var tpl bytes.Buffer
	err = tmpl.Execute(&tpl, t)
	if err != nil {
		panic(err)
	}

	return tpl.String()
}

func sendTemplate(esURL string, timeout int, templateSource, templateName string, numberOfShards, templateOrder int, dryRun bool) error {
	if dryRun {
		log.Infof("%s: dry run, skipping", templateName)
		return nil
	}
	url := esURL + "/_template/" + templateName
	tpl := getRenderedTemplate(templateSource, strconv.Itoa(numberOfShards), templateName, templateOrder)

	_, err := esQueryPut(url, timeout, tpl)
	if err != nil {
		return err
	}
	return nil
}

func deleteTemplate(esURL string, timeout int, templateName string, dryRun bool) error {
	if dryRun {
		log.Infof("%s: dry run, skipping", templateName)
		return nil
	}
	url := esURL + "/_template/" + templateName

	_, err := esQueryDelete(url, timeout)
	if err != nil {
		return err
	}
	return nil
}

func processData(esURL string, timeout int, shards []Shard, shardLimit, defaultShardNumber , maxDeltaThreshold int, templates map[string]Template, templateSource string, dryRun bool) error {
	calculatedShards, err := calculateNumerOfShards(shards, templates, shardLimit)
	if err != nil {
		return err
	}

	log.Infof("logstash: number of shards %v", defaultShardNumber)
	err = sendTemplate(esURL, timeout, templateSource, "logstash", defaultShardNumber, 0, dryRun)
	if err != nil {
		return err
	}

	i := 1
	for _, v := range calculatedShards {
		targetNumberOfShards := v["target_number_of_shards"].(int)
		if targetNumberOfShards == v["template_number_of_shards"].(int) {
			continue
		}
		// if targetNumberOfShards == 1 && v["template_number_of_shards"].(int) == 0 {
		if targetNumberOfShards <= defaultShardNumber && v["template_number_of_shards"].(int) == 0 {
			continue
		}
		if v["template_number_of_shards"].(int) == 0 {
			log.Infof(
				"%s: new individual template, number of shards %v",
				v["template_pattern"].(string),
				targetNumberOfShards,
			)
		} else {
			// descreasing number of shards
			if targetNumberOfShards < v["template_number_of_shards"].(int) {
				// calculating number of delta shards based on percentage
				maxDeltaShards := int(float64(v["template_number_of_shards"].(int)) * float64(maxDeltaThreshold) / 100)
				if maxDeltaShards < 1 {
					maxDeltaShards = 1
				}

				if v["template_number_of_shards"].(int) - targetNumberOfShards > maxDeltaShards {
					log.Infof(
						"%s: calculated number of shards decreased from %v to %v, it exceeds max delta threshold (%v%% ~ %v shards), setting number of shards to %v",
						v["template_pattern"].(string),
						v["template_number_of_shards"].(int),
						targetNumberOfShards,
						maxDeltaThreshold,
						maxDeltaShards,
						v["template_number_of_shards"].(int) - maxDeltaShards,
					)							
					targetNumberOfShards = v["template_number_of_shards"].(int) - maxDeltaShards
				// } else if targetNumberOfShards == 1 {
				} else if targetNumberOfShards <= defaultShardNumber {
					log.Infof(
						"%s: calculated number of shards is %v (less or equal to default number of shards %v), deleting individual template",
						v["template_pattern"].(string),
						targetNumberOfShards,
						defaultShardNumber,
					)
					err := deleteTemplate(esURL, timeout, v["template_pattern"].(string), dryRun)
					if err != nil {
						return err
					}
					continue
				}
			}
			log.Infof(
				"%s: change number of shards %v -> %v",
				v["template_pattern"].(string),
				v["template_number_of_shards"].(int),
				targetNumberOfShards,
			)
		}
		err = sendTemplate(
			esURL,
			timeout,
			templateSource,
			v["template_pattern"].(string),
			targetNumberOfShards,
			i,
			dryRun,
		)
		if err != nil {
			return err
		}
		i++
	}

	return nil
}

func main() {
	kingpin.Version(ver)
	kingpin.Parse()

	shards, err := getShards(*esURL, *timeout)
	if err != nil {
		panic(err)
	}
	templates, err := getTemplates(*esURL, *timeout)
	if err != nil {
		panic(err)
	}
	templateSource, err := readTemplateFile(*templateFilePath)
	if err != nil {
		panic(err)
	}

	err = processData(*esURL, *timeout, shards, *shardLimit, *defaultShardNumber, *maxDeltaThreshold, templates, templateSource, *dryRun)
	if err != nil {
		panic(err)
	}
}
