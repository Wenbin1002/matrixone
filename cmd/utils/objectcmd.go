// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectcmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"sort"
	"strconv"
	"strings"
)

var (
	level      int
	name       string
	id         int
	cols, rows []int
	col, row   string
)

const (
	brief    = 0
	standard = 1
	detailed = 2
)

func getInputs(input string, result *[]int) error {
	*result = make([]int, 0)
	if input == "" {
		return nil
	}
	items := strings.Split(input, ",")
	for _, item := range items {
		item = strings.TrimSpace(item)
		num, err := strconv.Atoi(item)
		if err != nil {
			return fmt.Errorf("invalid number '%s'", item)
		}
		*result = append(*result, num)
	}
	return nil
}

func GetCommands() (commands []*cobra.Command) {

	commands = append(commands, getStatCmd())
	commands = append(commands, getGetCmd())

	return
}

func getStatCmd() *cobra.Command {
	var statCmd = &cobra.Command{
		Use:   "stat",
		Short: "Perform a stat operation",
		Run: func(cmd *cobra.Command, args []string) {
			if level != brief && level != standard && level != detailed {
				cmd.OutOrStdout().Write(
					[]byte(fmt.Sprintf("\n invalid level %v, should be 0, 1, 2 ", level)),
				)
				return
			}
			cmd.OutOrStdout().Write(
				[]byte(fmt.Sprintf("\n level: %v\n name: %v\n ", level, name)),
			)
		},
	}
	statCmd.Flags().IntVarP(&level, "level", "l", 0, "level")
	statCmd.Flags().StringVarP(&name, "name", "n", "", "name")

	return statCmd
}

func getGetCmd() *cobra.Command {
	var getCmd = &cobra.Command{
		Use:   "get",
		Short: "Perform a get operation",
		Run: func(cmd *cobra.Command, args []string) {
			if err := getInputs(col, &cols); err != nil {
				cmd.OutOrStdout().Write(
					[]byte(fmt.Sprint(err)),
				)
				return
			}
			if err := getInputs(row, &rows); err != nil {
				cmd.OutOrStdout().Write(
					[]byte(fmt.Sprint(err)),
				)
				return
			}
			if len(rows) > 2 || (len(rows) == 2 && rows[0] >= rows[1]) {
				cmd.OutOrStdout().Write(
					[]byte(fmt.Sprint("\n invalid rows input, need one or two arguments\n")),
				)
				return
			}
			sort.Ints(cols)
			cmd.OutOrStdout().Write(
				[]byte(fmt.Sprintf("\n name: %v\n id: %v\n cols: %v\n rows: %v\n", name, id, cols, rows)),
			)
		},
	}
	getCmd.Flags().IntVarP(&id, "id", "i", 0, "id")
	getCmd.Flags().StringVarP(&name, "name", "n", "", "name")
	getCmd.Flags().StringVarP(&col, "col", "c", "", "col")
	getCmd.Flags().StringVarP(&row, "row", "r", "", "row")

	return getCmd
}
