// Copyright 2025 Sreram K (sreramk360@gmail.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package locks

// DetectDeadlock checks for cycles in the directed graph waitGraph
//   - Each "node" is a waiting transaction.
//   - This uses the `getNextDependingNode` function to get the node
//     the current node is waiting on, for a common resource. I.e.,
//     when currentNode is trying to lock a resource which node-2
//     has, then getNextDependingNode(currentNode) gives node-2's ID
//   - Because transactions do not wait on more than one resource
//     at a time, we don't need to implement depth first search (DFS).
//   - The current algorithm starts from the currentNode and checks for
//     cycle by waking the linked-list of transactions, associated/linked
//     by what resource they are writing on and what resource they
//     have acquired.
//   - There can be more than one transaction waiting on the same resource,
//     but there can be only one resource which has acquired it. Also, there
//     can be only one resource a specific transaction can wait for, at any
//     given time.
func DetectDeadlockCycle(
	startNode string,
	getNextDependingNode func(currentNode string) (nextNode string, ok bool),
) (cycleNodes []string, deadlockDetected bool) {
	nodeToPathIndxMap := map[string]int{}
	cyclePath := []string{startNode}

	for currentNode := startNode; ; {
		next, ok := getNextDependingNode(currentNode)
		if !ok {
			return nil, false
		}

		indx, ok := nodeToPathIndxMap[next]
		if ok {
			return cyclePath[indx:], true
		}

		nodeToPathIndxMap[next] = len(cyclePath)
		cyclePath = append(cyclePath, next)
		currentNode = next
	}

}
