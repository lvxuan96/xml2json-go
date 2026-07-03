package converter

import (
	"encoding/json"
	"testing"

	"xml2json-go/internal/config"
)

func TestXML2JSON_SimpleElement(t *testing.T) {
	opts := defaultOpts()
	conv := New(opts, nil)

	xmlData := `<root>hello</root>`
	jsonData, err := conv.Convert([]byte(xmlData))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 纯文本根元素: 直接返回文本内容
	expected := `"hello"`
	assertJSON(t, expected, string(jsonData))
}

func TestXML2JSON_Attributes(t *testing.T) {
	opts := defaultOpts()
	conv := New(opts, nil)

	xmlData := `<person name="张三" age="30" city="北京"/>`
	jsonData, err := conv.Convert([]byte(xmlData))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("result: %s", string(jsonData))

	// 根元素只有属性: 根名被剥离, 属性在顶层
	var result map[string]interface{}
	if err := json.Unmarshal(jsonData, &result); err != nil {
		t.Fatalf("invalid json output: %v", err)
	}

	if result["@name"] != "张三" {
		t.Errorf("expected @name=张三, got %v", result["@name"])
	}
	if result["@age"] != "30" {
		t.Errorf("expected @age=30, got %v", result["@age"])
	}
	if result["@city"] != "北京" {
		t.Errorf("expected @city=北京, got %v", result["@city"])
	}
}

func TestXML2JSON_RepeatedElements(t *testing.T) {
	opts := defaultOpts()
	conv := New(opts, nil)

	xmlData := `<items><item id="1">A</item><item id="2">B</item><item id="3">C</item></items>`
	jsonData, err := conv.Convert([]byte(xmlData))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("result: %s", string(jsonData))

	// 根元素被剥离, 重复子元素转为 JSON 数组
	var result map[string]interface{}
	if err := json.Unmarshal(jsonData, &result); err != nil {
		t.Fatalf("invalid json output: %v", err)
	}

	itemArr, ok := result["item"].([]interface{})
	if !ok {
		t.Fatalf("expected item to be an array, got %T", result["item"])
	}

	if len(itemArr) != 3 {
		t.Errorf("expected 3 items, got %d", len(itemArr))
	}

	// 验证第一个元素
	first, ok := itemArr[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected item[0] to be an object")
	}
	if first["@id"] != "1" {
		t.Errorf("expected @id=1, got %v", first["@id"])
	}
	if first["#text"] != "A" {
		t.Errorf("expected #text=A, got %v", first["#text"])
	}
}

func TestXML2JSON_NestedStructure(t *testing.T) {
	opts := defaultOpts()
	conv := New(opts, nil)

	xmlData := `
<order id="12345">
  <customer name="张三">
    <email>zhangsan@example.com</email>
  </customer>
  <items>
    <item sku="A001" qty="2">无线鼠标</item>
    <item sku="B002" qty="1">机械键盘</item>
  </items>
</order>`

	jsonData, err := conv.Convert([]byte(xmlData))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("result: %s", string(jsonData))

	// 验证完整嵌套结构
	var result map[string]interface{}
	if err := json.Unmarshal(jsonData, &result); err != nil {
		t.Fatalf("invalid json output: %v", err)
	}

	// 根元素的属性
	if result["@id"] != "12345" {
		t.Errorf("expected @id=12345, got %v", result["@id"])
	}

	// customer 嵌套对象
	customer, ok := result["customer"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected customer to be an object")
	}
	if customer["@name"] != "张三" {
		t.Errorf("expected @name=张三, got %v", customer["@name"])
	}
	if customer["email"] != "zhangsan@example.com" {
		t.Errorf("expected email=zhangsan@example.com, got %v", customer["email"])
	}

	// items → item 数组
	items, ok := result["items"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected items to be an object")
	}
	itemArr, ok := items["item"].([]interface{})
	if !ok {
		t.Fatalf("expected item to be an array")
	}
	if len(itemArr) != 2 {
		t.Errorf("expected 2 items, got %d", len(itemArr))
	}
}

func TestXML2JSON_EmptyDocument(t *testing.T) {
	opts := defaultOpts()
	conv := New(opts, nil)

	xmlData := ``
	_, err := conv.Convert([]byte(xmlData))
	if err == nil {
		t.Error("expected error for empty document")
	}
}

func TestXML2JSON_InvalidXML(t *testing.T) {
	opts := defaultOpts()
	opts.StrictMode = true
	conv := New(opts, nil)

	xmlData := `<root><unclosed></root>`
	_, err := conv.Convert([]byte(xmlData))
	if err == nil {
		t.Error("expected error for invalid XML in strict mode")
	}
}

func TestXML2JSON_CustomPrefix(t *testing.T) {
	opts := defaultOpts()
	opts.AttributePrefix = "" // 无属性前缀
	opts.TextKey = "_text"    // 自定义文本 key
	conv := New(opts, nil)

	xmlData := `<greeting lang="en">Hello</greeting>`
	jsonData, err := conv.Convert([]byte(xmlData))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("result: %s", string(jsonData))

	var result map[string]interface{}
	if err := json.Unmarshal(jsonData, &result); err != nil {
		t.Fatalf("invalid json output: %v", err)
	}

	// 根元素被剥离, 直接暴露属性和文本
	if result["lang"] != "en" {
		t.Errorf("expected lang=en, got %v", result["lang"])
	}
	if result["_text"] != "Hello" {
		t.Errorf("expected _text=Hello, got %v", result["_text"])
	}
}

func TestXML2JSON_CDATA(t *testing.T) {
	opts := defaultOpts()
	conv := New(opts, nil)

	xmlData := `<root><content><![CDATA[<html>hello</html>]]></content></root>`
	jsonData, err := conv.Convert([]byte(xmlData))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("result: %s", string(jsonData))

	var result map[string]interface{}
	if err := json.Unmarshal(jsonData, &result); err != nil {
		t.Fatalf("invalid json output: %v", err)
	}

	// CDATA 作为文本内容保留
	if result["content"] != "<html>hello</html>" {
		t.Errorf("expected CDATA content, got %v", result["content"])
	}
}

func TestXML2JSON_NonStrictMode(t *testing.T) {
	opts := defaultOpts()
	opts.StrictMode = false
	conv := New(opts, nil)

	// 非严格模式下, 格式错误不中断
	xmlData := `<root><ok>fine</ok><bad>missing end</root>`
	jsonData, err := conv.Convert([]byte(xmlData))
	if err != nil {
		t.Fatalf("unexpected error in non-strict mode: %v", err)
	}

	t.Logf("non-strict result: %s", string(jsonData))
}

func TestXML2JSON_BrokenTagsWithSpaces(t *testing.T) {
	// 测试预处理：标签名含空格的畸形 XML 自动修复
	opts := defaultOpts()
	conv := New(opts, nil)

	xmlData := `<?xml version="1.0" encoding="UTF-8" ?>
<ROWSET>
<ROW>
<order id>12345</order id>
<customer name>张三</customer name>
<item>无线鼠标</item>
</ROW>
</ROWSET>`

	jsonData, err := conv.Convert([]byte(xmlData))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("fixed result: %s", string(jsonData))

	var result map[string]interface{}
	if err := json.Unmarshal(jsonData, &result); err != nil {
		t.Fatalf("invalid json output: %v", err)
	}

	// ROWSET → ROW
	rowset, ok := result["ROW"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected ROW to be an object, got %T", result["ROW"])
	}

	// 验证修复后的标签名
	if rowset["order_id"] != "12345" {
		t.Errorf("expected order_id=12345, got %v", rowset["order_id"])
	}
	if rowset["customer_name"] != "张三" {
		t.Errorf("expected customer_name=张三, got %v", rowset["customer_name"])
	}
	if rowset["item"] != "无线鼠标" {
		t.Errorf("expected item=无线鼠标, got %v", rowset["item"])
	}
}

func assertJSON(t *testing.T, expected, actual string) {
	t.Helper()

	var exp, act interface{}
	if err := json.Unmarshal([]byte(expected), &exp); err != nil {
		t.Fatalf("invalid expected JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(actual), &act); err != nil {
		t.Fatalf("invalid actual JSON: %v (raw: %s)", err, actual)
	}

	expBytes, _ := json.Marshal(exp)
	actBytes, _ := json.Marshal(act)

	if string(expBytes) != string(actBytes) {
		t.Errorf("JSON mismatch:\n  expected: %s\n  actual:   %s", string(expBytes), string(actBytes))
	}
}

func defaultOpts() *config.TransformConfig {
	return &config.TransformConfig{
		AttributePrefix: "@",
		TextKey:         "#text",
		CDataKey:        "#cdata",
		NamespaceMode:   "strip",
		TrimElements:    true,
		SkipComments:    true,
		SkipProcInst:    true,
	}
}
