package converter

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"strings"
	"sync"

	"xml2json-go/internal/config"

	"go.uber.org/zap"
)

// XML2JSON XML 到 JSON 转换器
type XML2JSON struct {
	opts    *config.TransformConfig
	bufPool sync.Pool
	logger  *zap.Logger
}

// New 创建转换器
func New(opts *config.TransformConfig, logger *zap.Logger) *XML2JSON {
	return &XML2JSON{
		opts:   opts,
		logger: logger,
		bufPool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, 4096))
			},
		},
	}
}

// xmlNode XML 中间节点
type xmlNode struct {
	Name       string
	Attrs      map[string]string
	Children   []*xmlNode
	Text       string
	Namespace  string
}

// Convert 将 XML 字节数据转换为 JSON 字节数据
func (c *XML2JSON) Convert(xmlData []byte) ([]byte, error) {
	decoder := xml.NewDecoder(bytes.NewReader(xmlData))
	decoder.Strict = c.opts.StrictMode

	// 解析 XML 为中间节点树
	root := &xmlNode{}
	var current *xmlNode
	stack := []*xmlNode{}

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			if c.opts.StrictMode {
				return nil, fmt.Errorf("xml decode error: %w", err)
			}
			c.logger.Warn("xml decode error (non-strict, skipping)", zap.Error(err))
			continue
		}

		switch t := token.(type) {
		case xml.StartElement:
			node := &xmlNode{
				Name: t.Name.Local,
				Attrs: make(map[string]string),
			}

			// 处理命名空间
			if t.Name.Space != "" && c.opts.NamespaceMode == "keep" {
				node.Name = t.Name.Space + ":" + t.Name.Local
			}

			// 处理属性
			for _, attr := range t.Attr {
				if c.opts.NamespaceMode == "keep" && attr.Name.Space != "" {
					node.Attrs[c.opts.AttributePrefix+attr.Name.Space+":"+attr.Name.Local] = attr.Value
				} else {
					node.Attrs[c.opts.AttributePrefix+attr.Name.Local] = attr.Value
				}
			}

			if root.Name == "" && len(stack) == 0 {
				root = node
				current = node
			} else if current != nil {
				current.Children = append(current.Children, node)
			}
			stack = append(stack, node)
			current = node

		case xml.EndElement:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
				if len(stack) > 0 {
					current = stack[len(stack)-1]
				}
			}

		case xml.CharData:
			text := strings.TrimSpace(string(t))
			if text == "" {
				continue
			}
			if current != nil {
				if current.Text == "" {
					current.Text = text
				} else {
					current.Text += text
				}
			}

		case xml.Comment:
			if !c.opts.SkipComments && current != nil {
				current.Children = append(current.Children, &xmlNode{
					Name: "#comment",
					Text: strings.TrimSpace(string(t)),
				})
			}

		case xml.ProcInst:
			// 跳过处理指令

		case xml.Directive:
			// 跳过 DOCTYPE 等
		}
	}

	if root.Name == "" {
		return nil, fmt.Errorf("empty xml document or parse error")
	}

	// 转为 JSON 结构
	result := c.nodeToJSON(root)

	buf := c.bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.bufPool.Put(buf)

	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(result); err != nil {
		return nil, fmt.Errorf("json encode error: %w", err)
	}

	// 去掉末尾换行
	output := make([]byte, buf.Len())
	copy(output, buf.Bytes())

	// 去掉 json.Encoder 自动添加的 '\n'
	if len(output) > 0 && output[len(output)-1] == '\n' {
		output = output[:len(output)-1]
	}

	return output, nil
}

// nodeToJSON 将中间节点转为 JSON 兼容的 interface{}
func (c *XML2JSON) nodeToJSON(node *xmlNode) interface{} {
	// 叶子节点：只有文本和属性
	hasChildren := len(node.Children) > 0
	hasAttrs := len(node.Attrs) > 0
	hasText := node.Text != ""

	// 纯文本叶子节点
	if !hasChildren && !hasAttrs && hasText {
		return node.Text
	}

	// 复杂节点
	result := make(map[string]interface{})

	// 添加属性
	for k, v := range node.Attrs {
		result[k] = v
	}

	// 分组子元素
	childGroups := make(map[string][]*xmlNode)
	for _, child := range node.Children {
		childGroups[child.Name] = append(childGroups[child.Name], child)
	}

	// 处理子元素
	for name, group := range childGroups {
		if len(group) == 1 {
			result[name] = c.nodeToJSON(group[0])
		} else {
			arr := make([]interface{}, 0, len(group))
			for _, child := range group {
				arr = append(arr, c.nodeToJSON(child))
			}
			result[name] = arr
		}
	}

	// 添加文本内容
	if hasText {
		result[c.opts.TextKey] = node.Text
	}

	return result
}

// Preview 预览转换结果（供 API 使用）
func (c *XML2JSON) Preview(xmlData []byte) (map[string]interface{}, error) {
	jsonBytes, err := c.Convert(xmlData)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		return nil, fmt.Errorf("json unmarshal error: %w", err)
	}
	return result, nil
}
