// MIT License
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE

package algorithm

import (
	"path/filepath"
	"io/ioutil"
	"testing"
	"strings"
)

var testYamlRootPath, _ = filepath.Abs("../../test")
var groupASettingPath = filepath.Join(testYamlRootPath, "group_A_setting.yaml")
var groupBSettingPath = filepath.Join(testYamlRootPath, "group_B_setting.yaml")

func TestHivedAlgorithmGroupA(t *testing.T) {
	fileInfo, err := ioutil.ReadDir(testYamlRootPath)
	if err != nil {
		panic(err)
	}
	for _, file := range fileInfo {
		if (file.IsDir() == false) && (strings.HasPrefix(file.Name(), "group_A_case_")) {
			caseFileName := file.Name()
			caseFilePath := filepath.Join(testYamlRootPath, caseFileName)
			t.Logf("Will execute %v for group A", caseFileName)
			tester := NewHivedAlgorithmTester(t, groupASettingPath)
			tester.ExecuteCaseFromYamlFile(caseFilePath)
		}
	}
}

func TestHivedAlgorithmGroupB(t *testing.T) {
	fileInfo, err := ioutil.ReadDir(testYamlRootPath)
	if err != nil {
		panic(err)
	}
	for _, file := range fileInfo {
		if (file.IsDir() == false) && (strings.HasPrefix(file.Name(), "group_B_case_")) {
			caseFileName := file.Name()
			caseFilePath := filepath.Join(testYamlRootPath, caseFileName)
			t.Logf("Will execute %v for group B", caseFileName)
			tester := NewHivedAlgorithmTester(t, groupBSettingPath)
			tester.ExecuteCaseFromYamlFile(caseFilePath)
		}
	}
}