// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

// Bit represents the location of a single bit.
type Bit struct {
	RowID     uint64
	ColumnID  uint64
	RowKey    string
	ColumnKey string
	Timestamp int64
}

// Bits represents a slice of bits.
type Bits []Bit

func (p Bits) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p Bits) Len() int      { return len(p) }

func (p Bits) Less(i, j int) bool {
	if p[i].RowID == p[j].RowID {
		if p[i].ColumnID < p[j].ColumnID {
			return p[i].Timestamp < p[j].Timestamp
		}
		return p[i].ColumnID < p[j].ColumnID
	}
	return p[i].RowID < p[j].RowID
}

// RowIDs returns a slice of all the row IDs.
func (p Bits) RowIDs() []uint64 {
	other := make([]uint64, len(p))
	for i := range p {
		other[i] = p[i].RowID
	}
	return other
}

// ColumnIDs returns a slice of all the column IDs.
func (p Bits) ColumnIDs() []uint64 {
	other := make([]uint64, len(p))
	for i := range p {
		other[i] = p[i].ColumnID
	}
	return other
}

// RowKeys returns a slice of all the row keys.
func (p Bits) RowKeys() []string {
	other := make([]string, len(p))
	for i := range p {
		other[i] = p[i].RowKey
	}
	return other
}

// ColumnKeys returns a slice of all the column keys.
func (p Bits) ColumnKeys() []string {
	other := make([]string, len(p))
	for i := range p {
		other[i] = p[i].ColumnKey
	}
	return other
}

// Timestamps returns a slice of all the timestamps.
func (p Bits) Timestamps() []int64 {
	other := make([]int64, len(p))
	for i := range p {
		other[i] = p[i].Timestamp
	}
	return other
}

// GroupBySlice returns a map of bits by slice.
func (p Bits) GroupBySlice() map[uint64][]Bit {
	m := make(map[uint64][]Bit)
	for _, bit := range p {
		slice := bit.ColumnID / SliceWidth
		m[slice] = append(m[slice], bit)
	}

	for slice, bits := range m {
		sort.Sort(Bits(bits))
		m[slice] = bits
	}

	return m
}

// FieldValues represents the value for a column within a
// range-encoded frame.
type FieldValue struct {
	ColumnID uint64
	Value    int64
}

// FieldValues represents a slice of field values.
type FieldValues []FieldValue

func (p FieldValues) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p FieldValues) Len() int      { return len(p) }

func (p FieldValues) Less(i, j int) bool {
	return p[i].ColumnID < p[j].ColumnID
}

// ColumnIDs returns a slice of all the column IDs.
func (p FieldValues) ColumnIDs() []uint64 {
	other := make([]uint64, len(p))
	for i := range p {
		other[i] = p[i].ColumnID
	}
	return other
}

// Values returns a slice of all the values.
func (p FieldValues) Values() []int64 {
	other := make([]int64, len(p))
	for i := range p {
		other[i] = p[i].Value
	}
	return other
}

// GroupBySlice returns a map of field values by slice.
func (p FieldValues) GroupBySlice() map[uint64][]FieldValue {
	m := make(map[uint64][]FieldValue)
	for _, val := range p {
		slice := val.ColumnID / SliceWidth
		m[slice] = append(m[slice], val)
	}

	for slice, vals := range m {
		sort.Sort(FieldValues(vals))
		m[slice] = vals
	}

	return m
}

// BitsByPos represents a slice of bits sorted by internal position.
type BitsByPos []Bit

func (p BitsByPos) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p BitsByPos) Len() int      { return len(p) }
func (p BitsByPos) Less(i, j int) bool {
	p0, p1 := Pos(p[i].RowID, p[i].ColumnID), Pos(p[j].RowID, p[j].ColumnID)
	if p0 == p1 {
		return p[i].Timestamp < p[j].Timestamp
	}
	return p0 < p1
}

// InternalClient should be implemented by any struct that enables any transport between nodes
// TODO: Refactor
// Note from Travis: Typically an interface containing more than two or three methods is an indication that
// something hasn't been architected correctly.
// While I understand that putting the entire Client behind an interface might require this many methods,
// I don't want to let it go unquestioned.
type InternalClient interface {
	MaxSliceByIndex(ctx context.Context) (map[string]uint64, error)
	MaxInverseSliceByIndex(ctx context.Context) (map[string]uint64, error)
	Schema(ctx context.Context) ([]*IndexInfo, error)
	CreateIndex(ctx context.Context, index string, opt IndexOptions) error
	FragmentNodes(ctx context.Context, index string, slice uint64) ([]*Node, error)
	Query(ctx context.Context, index string, queryRequest *internal.QueryRequest) (*internal.QueryResponse, error)
	QueryNode(ctx context.Context, uri *URI, index string, queryRequest *internal.QueryRequest) (*internal.QueryResponse, error)
	Import(ctx context.Context, index, frame string, slice uint64, bits []Bit) error
	ImportK(ctx context.Context, index, frame string, bits []Bit) error
	EnsureIndex(ctx context.Context, name string, options IndexOptions) error
	EnsureFrame(ctx context.Context, indexName string, frameName string, options FrameOptions) error
	ImportValue(ctx context.Context, index, frame, field string, slice uint64, vals []FieldValue) error
	ExportCSV(ctx context.Context, index, frame, view string, slice uint64, w io.Writer) error
	BackupTo(ctx context.Context, w io.Writer, index, frame, view string) error
	BackupSlice(ctx context.Context, index, frame, view string, slice uint64) (io.ReadCloser, error)
	RestoreFrom(ctx context.Context, r io.Reader, index, frame, view string) error
	CreateFrame(ctx context.Context, index, frame string, opt FrameOptions) error
	RestoreFrame(ctx context.Context, host, index, frame string) error
	FrameViews(ctx context.Context, index, frame string) ([]string, error)
	FragmentBlocks(ctx context.Context, index, frame, view string, slice uint64) ([]FragmentBlock, error)
	BlockData(ctx context.Context, index, frame, view string, slice uint64, block int) ([]uint64, []uint64, error)
	ColumnAttrDiff(ctx context.Context, index string, blks []AttrBlock) (map[uint64]map[string]interface{}, error)
	RowAttrDiff(ctx context.Context, index, frame string, blks []AttrBlock) (map[uint64]map[string]interface{}, error)
	SendMessage(ctx context.Context, uri *URI, pb proto.Message) error
}

type ExternalClient interface {
	ArchiveFrame(ctx context.Context, w io.Writer, index, frame, view string) error
	BackupSlice(ctx context.Context, index, frame, view string, slice uint64) (io.ReadCloser, error)
	EnsureFrame(ctx context.Context, indexName string, frameName string, options FrameOptions) error
	EnsureIndex(ctx context.Context, name string, options IndexOptions) error
	ExportCSV(ctx context.Context, index, frame, view string, slice uint64, w io.Writer) error
	FrameViews(ctx context.Context, index, frame string) ([]string, error)
	Import(ctx context.Context, index, frame string, slice uint64, bits []Bit) error
	ImportK(ctx context.Context, index, frame string, bits []Bit) error
	ImportValue(ctx context.Context, index, frame, field string, slice uint64, vals []FieldValue) error
	MaxInverseSliceByIndex(ctx context.Context) (map[string]uint64, error)
	MaxSliceByIndex(ctx context.Context) (map[string]uint64, error)
	Query(ctx context.Context, index string, queryRequest *internal.QueryRequest) (*internal.QueryResponse, error)
	RestoreFrame(ctx context.Context, r io.Reader, index, frame, view string) error
	//RetrieveSliceFromURI(ctx context.Context, index, frame, view string, slice uint64) (io.ReadCloser, error) {
}

/*
	//FragmentNodes(ctx context.Context, index string, slice uint64) ([]*Node, error) // TODO: made this unexported
	//--BlockData(ctx context.Context, index, frame, view string, slice uint64, block int) ([]uint64, []uint64, error)
	//--ColumnAttrDiff(ctx context.Context, index string, blks []AttrBlock) (map[uint64]map[string]interface{}, error)
	CreateFrame(ctx context.Context, index, frame string, opt FrameOptions) error
	CreateIndex(ctx context.Context, index string, opt IndexOptions) error
	//--EnsureFrame(ctx context.Context, indexName string, frameName string, options FrameOptions) error
	//--EnsureIndex(ctx context.Context, name string, options IndexOptions) error
	//--FragmentBlocks(ctx context.Context, index, frame, view string, slice uint64) ([]FragmentBlock, error)
	RestoreFrame(ctx context.Context, host, index, frame string) error
	//--RowAttrDiff(ctx context.Context, index, frame string, blks []AttrBlock) (map[uint64]map[string]interface{}, error)
	Schema(ctx context.Context) ([]*IndexInfo, error)
	//--SendMessage(ctx context.Context, uri *URI, pb proto.Message) error
*/

// ExternalHTTPClient represents a client of the Pilosa cluster.
type ExternalHTTPClient struct {
	commonAPI
	uri    *URI
	client *http.Client
}

// NewExternalHTTPClient returns a new instance of ExternalHTTPClient to connect to host.
func NewExternalHTTPClient(host string, client *http.Client) (*ExternalHTTPClient, error) {
	if host == "" {
		return nil, ErrHostRequired
	}

	uri, err := NewURIFromAddress(host)
	if err != nil {
		return nil, err
	}

	return &ExternalHTTPClient{
		uri:    uri,
		client: client,
	}, nil
}

// ArchiveFrame writes an entire frame/view as a tar file to w.
func (c *ExternalHTTPClient) ArchiveFrame(ctx context.Context, w io.Writer, index, frame, view string) error {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	// Create tar writer around writer.
	tw := tar.NewWriter(w)

	// Find the maximum number of slices.
	var maxSlices map[string]uint64
	var err error
	if view == ViewStandard {
		maxSlices, err = c.maxSliceByIndex(ctx, false)
	} else if view == ViewInverse {
		maxSlices, err = c.maxSliceByIndex(ctx, true)
	} else {
		return ErrInvalidView
	}

	if err != nil {
		return fmt.Errorf("slice n: %s", err)
	}

	// Backup every slice to the tar file.
	for i := uint64(0); i <= maxSlices[index]; i++ {
		if err := c.archiveSlice(ctx, tw, index, frame, view, i); err != nil {
			return err
		}
	}

	// Close tar file.
	if err := tw.Close(); err != nil {
		return err
	}

	return nil
}

// archiveSlice backs up a single slice to tw.
func (c *ExternalHTTPClient) archiveSlice(ctx context.Context, tw *tar.Writer, index, frame, view string, slice uint64) error {
	// Return error if unable to backup from any slice.
	r, err := c.BackupSlice(ctx, index, frame, view, slice)
	if err != nil {
		return fmt.Errorf("backup slice: slice=%d, err=%s", slice, err)
	} else if r == nil {
		return nil
	}
	defer r.Close()

	// Read entire buffer to determine file size.
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	} else if err := r.Close(); err != nil {
		return err
	}

	// Write slice file header.
	if err := tw.WriteHeader(&tar.Header{
		Name:    strconv.FormatUint(slice, 10),
		Mode:    0666,
		Size:    int64(len(data)),
		ModTime: time.Now(),
	}); err != nil {
		return err
	}

	// Write buffer to file.
	if _, err := tw.Write(data); err != nil {
		return fmt.Errorf("write buffer: %s", err)
	}

	return nil
}

// ExportCSV bulk exports data for a single slice from a host to CSV format.
func (c *ExternalHTTPClient) ExportCSV(ctx context.Context, index, frame, view string, slice uint64, w io.Writer) error {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	} else if !(view == ViewStandard || view == ViewInverse) {
		return ErrInvalidView
	}

	// Retrieve a list of nodes that own the slice.
	nodes, err := c.fragmentNodes(ctx, index, slice)
	if err != nil {
		return fmt.Errorf("slice nodes: %s", err)
	}

	// Attempt nodes in random order.
	var e error
	for _, i := range rand.Perm(len(nodes)) {
		node := nodes[i]

		if err := c.exportNodeCSV(ctx, node, index, frame, view, slice, w); err != nil {
			e = fmt.Errorf("export node: host=%s, err=%s", node.URI, err)
			continue
		} else {
			return nil
		}
	}

	return e
}

// exportNode copies a CSV export from a node to w.
func (c *ExternalHTTPClient) exportNodeCSV(ctx context.Context, node *Node, index, frame, view string, slice uint64, w io.Writer) error {
	// Create URL.
	u := nodePathToURL(node, "/export")
	u.RawQuery = url.Values{
		"index": {index},
		"frame": {frame},
		"view":  {view},
		"slice": {strconv.FormatUint(slice, 10)},
	}.Encode()

	// Generate HTTP request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/csv")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.client.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Validate status code.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status: %d", resp.StatusCode)
	}

	// Copy body to writer.
	if _, err := io.Copy(w, resp.Body); err != nil {
		return err
	}

	return nil
}

// FrameViews returns a list of view names for a frame.
func (c *ExternalHTTPClient) FrameViews(ctx context.Context, index, frame string) ([]string, error) {
	// Create URL & HTTP request.
	u := uriPathToURL(c.uri, fmt.Sprintf("/index/%s/frame/%s/views", index, frame))
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Handle response based on status code.
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, ErrFrameNotFound
	default:
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.New(string(body))
	}

	// Decode response.
	var rsp getFrameViewsResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, err
	}
	return rsp.Views, nil
}

// Import bulk imports bits for a single slice to a host.
func (c *ExternalHTTPClient) Import(ctx context.Context, index, frame string, slice uint64, bits []Bit) error {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	buf, err := marshalImportPayload(index, frame, slice, bits)
	if err != nil {
		return fmt.Errorf("Error Creating Payload: %s", err)
	}

	// Retrieve a list of nodes that own the slice.
	nodes, err := c.fragmentNodes(ctx, index, slice)
	if err != nil {
		return fmt.Errorf("slice nodes: %s", err)
	}

	// Import to each node.
	for _, node := range nodes {
		if err := c.importNode(ctx, node, buf); err != nil {
			return fmt.Errorf("import node: host=%s, err=%s", node.URI, err)
		}
	}

	return nil
}

// ImportK bulk imports bits to a host.
func (c *ExternalHTTPClient) ImportK(ctx context.Context, index, frame string, bits []Bit) error {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	buf, err := marshalImportPayloadK(index, frame, bits)
	if err != nil {
		return fmt.Errorf("Error Creating Payload: %s", err)
	}

	node := &Node{
		URI: *c.uri,
	}

	// Import to node.
	if err := c.importNode(ctx, node, buf); err != nil {
		return fmt.Errorf("import node: host=%s, err=%s", node.URI, err)
	}

	return nil
}

// importNode sends a pre-marshaled import request to a node.
func (c *ExternalHTTPClient) importNode(ctx context.Context, node *Node, buf []byte) error {
	// Create URL & HTTP request.
	u := nodePathToURL(node, "/import")
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.client.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	} else if resp.StatusCode != http.StatusOK {
		return errors.New(string(body))
	}

	var isresp internal.ImportResponse
	if err := proto.Unmarshal(body, &isresp); err != nil {
		return fmt.Errorf("unmarshal import response: %s", err)
	} else if s := isresp.Err; s != "" {
		return errors.New(s)
	}

	return nil
}

// ImportValue bulk imports field values for a single slice to a host.
func (c *ExternalHTTPClient) ImportValue(ctx context.Context, index, frame, field string, slice uint64, vals []FieldValue) error {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	buf, err := marshalImportValuePayload(index, frame, field, slice, vals)
	if err != nil {
		return fmt.Errorf("Error Creating Payload: %s", err)
	}

	// Retrieve a list of nodes that own the slice.
	nodes, err := c.fragmentNodes(ctx, index, slice)
	if err != nil {
		return fmt.Errorf("slice nodes: %s", err)
	}

	// Import to each node.
	for _, node := range nodes {
		if err := c.importValueNode(ctx, node, buf); err != nil {
			return fmt.Errorf("import node: host=%s, err=%s", node.URI, err)
		}
	}

	return nil
}

// importValueNode sends a pre-marshaled import request to a node.
func (c *ExternalHTTPClient) importValueNode(ctx context.Context, node *Node, buf []byte) error {
	// Create URL & HTTP request.
	u := nodePathToURL(node, "/import-value")
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.client.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	} else if resp.StatusCode != http.StatusOK {
		return errors.New(string(body))
	}

	var isresp internal.ImportResponse
	if err := proto.Unmarshal(body, &isresp); err != nil {
		return fmt.Errorf("unmarshal import response: %s", err)
	} else if s := isresp.Err; s != "" {
		return errors.New(s)
	}

	return nil
}

// MaxInverseSliceByIndex returns the number of inverse slices on a server by index.
func (c *ExternalHTTPClient) MaxInverseSliceByIndex(ctx context.Context) (map[string]uint64, error) {
	return c.maxSliceByIndex(ctx, true)
}

// MaxSliceByIndex returns the number of slices on a server by index.
func (c *ExternalHTTPClient) MaxSliceByIndex(ctx context.Context) (map[string]uint64, error) {
	return c.maxSliceByIndex(ctx, false)
}

// maxSliceByIndex returns the number of slices on a server by index.
func (c *ExternalHTTPClient) maxSliceByIndex(ctx context.Context, inverse bool) (map[string]uint64, error) {
	// Execute request against the host.
	u := uriPathToURL(c.uri, "/slices/max")

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request.
	resp, err := c.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rsp getSlicesMaxResponse
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http: status=%d", resp.StatusCode)
	} else if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}

	if inverse {
		return rsp.Inverse, nil
	}
	return rsp.Standard, nil
}

// RestoreFrom restores a frame from a backup file to an entire cluster.
func (c *ExternalHTTPClient) RestoreFrame(ctx context.Context, r io.Reader, index, frame, view string) error {
	if index == "" {
		return ErrIndexRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	// Create tar reader around input.
	tr := tar.NewReader(r)

	// Process each file.
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		// Parse slice from entry name.
		slice, err := strconv.ParseUint(hdr.Name, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid backup entry: %s", hdr.Name)
		}

		// Read file into buffer.
		var buf bytes.Buffer
		if _, err := io.CopyN(&buf, tr, hdr.Size); err != nil {
			return err
		}

		// Restore file to all nodes that own it.
		if err := c.restoreSlice(ctx, buf.Bytes(), index, frame, view, slice); err != nil {
			return err
		}
	}
}

// restoreSlice restores a single slice to all owning nodes.
func (c *ExternalHTTPClient) restoreSlice(ctx context.Context, buf []byte, index, frame, view string, slice uint64) error {
	// Retrieve a list of nodes that own the slice.
	nodes, err := c.fragmentNodes(ctx, index, slice)
	if err != nil {
		return fmt.Errorf("slice nodes: %s", err)
	}

	// Restore slice to each owner.
	for _, node := range nodes {
		u := nodePathToURL(node, "/fragment/data")
		u.RawQuery = url.Values{
			"index": {index},
			"frame": {frame},
			"view":  {view},
			"slice": {strconv.FormatUint(slice, 10)},
		}.Encode()

		// Build request.
		req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("User-Agent", "pilosa/"+Version)

		resp, err := c.client.Do(req.WithContext(ctx))
		if err != nil {
			return err
		}
		resp.Body.Close()

		// Return error if response not OK.
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: host=%s, code=%d", node.URI, resp.StatusCode)
		}
	}

	return nil
}

// Eventually this should be moved to nodeAPI and the code in cluster.go should use node.api.RetrieveSlice.
func (c *ExternalHTTPClient) RetrieveSlice(ctx context.Context, index, frame, view string, slice uint64) (io.ReadCloser, error) {
	node := &Node{
		URI: *c.uri,
	}
	return c.backupSliceNode(ctx, index, frame, view, slice, node)
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Helper functions

// marshalImportPayload marshalls the import parameters into a protobuf byte slice.
func marshalImportPayload(index, frame string, slice uint64, bits []Bit) ([]byte, error) {
	// Separate row and column IDs to reduce allocations.
	rowIDs := Bits(bits).RowIDs()
	columnIDs := Bits(bits).ColumnIDs()
	timestamps := Bits(bits).Timestamps()

	// Marshal bits to protobufs.
	buf, err := proto.Marshal(&internal.ImportRequest{
		Index:      index,
		Frame:      frame,
		Slice:      slice,
		RowIDs:     rowIDs,
		ColumnIDs:  columnIDs,
		Timestamps: timestamps,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal import request: %s", err)
	}
	return buf, nil
}

// marshalImportPayloadK marshalls the import parameters into a protobuf byte slice.
func marshalImportPayloadK(index, frame string, bits []Bit) ([]byte, error) {
	// Separate row and column IDs to reduce allocations.
	rowKeys := Bits(bits).RowKeys()
	columnKeys := Bits(bits).ColumnKeys()
	timestamps := Bits(bits).Timestamps()

	// Marshal bits to protobufs.
	buf, err := proto.Marshal(&internal.ImportRequest{
		Index:      index,
		Frame:      frame,
		RowKeys:    rowKeys,
		ColumnKeys: columnKeys,
		Timestamps: timestamps,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal import request: %s", err)
	}
	return buf, nil
}

// marshalImportValuePayload marshalls the import parameters into a protobuf byte slice.
func marshalImportValuePayload(index, frame, field string, slice uint64, vals []FieldValue) ([]byte, error) {
	// Separate row and column IDs to reduce allocations.
	columnIDs := FieldValues(vals).ColumnIDs()
	values := FieldValues(vals).Values()

	// Marshal bits to protobufs.
	buf, err := proto.Marshal(&internal.ImportValueRequest{
		Index:     index,
		Frame:     frame,
		Slice:     slice,
		Field:     field,
		ColumnIDs: columnIDs,
		Values:    values,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal import request: %s", err)
	}
	return buf, nil
}
