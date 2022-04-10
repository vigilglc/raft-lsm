package config

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestHelpIterateAllKeys(t *testing.T) {
	tp := reflect.TypeOf(ServerConfig{})
	var fldNames []string
	var cfgKeyNames []string
	var fldName2CapLower = func(name string) string {
		return strings.ToLower(name[:1]) + name[1:]
	}
	var fldName2CfgKeyName = func(name string) string {
		return fmt.Sprintf("configKey%s", name)
	}

	for i := 0; i < tp.NumField(); i++ {
		fld := tp.FieldByIndex([]int{i})
		if fld.IsExported() {
			name := fld.Name
			fldNames = append(fldNames, name)
			cfgKeyNames = append(cfgKeyNames, fldName2CfgKeyName(name))
			fmt.Printf("%s = \"%s\"\n", fldName2CfgKeyName(name), fldName2CapLower(name))
		}
	}
	fmt.Printf("configKeyNames = []string{%s}\n", strings.Join(cfgKeyNames, ","))

	for _, n := range fldNames {
		fmt.Printf("case %s:\n base.%s = incr.%s\n", fldName2CfgKeyName(n), n, n)
	}
}

func TestDefaultServerConfig(t *testing.T) {
	defCfg := DefaultServerConfig()
	t.Logf("%+v", defCfg)
	if err := Validate(defCfg); err != nil {
		t.Logf("error validated: %v\n", err)
	}
}

func TestReadServerConfig(t *testing.T) {
	cfg, err := ReadServerConfig("./test.toml")
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(cfg); err != nil {
		t.Fatalf("error validated: %v\n", err)
	}
	cfg.MakeLogger()
	cfg.GetLogger().Info("success")
}
