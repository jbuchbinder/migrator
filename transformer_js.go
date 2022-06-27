package migrator

import (
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
)

var (
	ottoInitialized bool
	ottoObj         *otto.Otto
)

func init() {
	TransformerMap["js"] = TableRenamerJavascript
}

// TableRenamerTransformer adjusts the table name of a destination table based
// on the "TableName" parameter passed.
var TableRenamerJavascript = func(dbName, tableName string, data []SQLRow, params *Parameters) []TableData {
	debug := paramBool(*params, ParamDebug, false)

	method, ok := (*params)[ParamMethod].(string)
	if !ok {
		method = ""
	}

	start := time.Now()
	defer func() {
		duration := time.Since(start)
		if caught := recover(); caught != nil {
			if caught == errHalt {
				logger.Warnf("transformer[js]: Timed out after %d sec", timeout)
				return
			}
			panic(caught)
		}
		logger.Debugf("transformer[js]: Completed execution in %v", duration)
	}()

	obj.vm.Interrupt = make(chan func(), 1)
	go func(timeout int) {
		time.Sleep(time.Duration(timeout) * time.Second)
		ottoObj.Interrupt <- func() {
			panic(errHalt)
		}
	}(timeout)

	log.Printf("transformer[js]: Beginning execution")
	_, err := ottoObj.Run(code)
	if err != nil {
		log.Warnf("transformer[js]: Returned %#v", err)
	}

	return []TableData{
		{
			DbName:    dbName,
			TableName: tableName,
			Data:      data,
			Method:    method,
		},
	}
}

func initializeJsEnvironment() error {
	if ottoInitialized {
		return nil
	}
	ottoObj = otto.New()

	ottoObj.vm.Set("log", func(call otto.FunctionCall) otto.Value {
		passedVal, _ := call.Argument(0).ToString()
		logger.Debugf("transformer[js]: %s: %s", obj.user.Name, passedVal)
		return otto.Value{}
	})

	ottoInitialized = true
	return nil
}
