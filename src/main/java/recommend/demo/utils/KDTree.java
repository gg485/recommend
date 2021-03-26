package recommend.demo.utils;

import java.util.ArrayList;
import java.util.List;

public class KDTree {

    //构建kd树,返回根
    public Node build(List<Node> nodeList, int index) {
        if (nodeList == null || nodeList.size() == 0)
            return null;
        Node root = findMedian(nodeList, index);
        root.dim = index;
        List<Node> leftNodeList = new ArrayList<>();
        List<Node> rightNodeList = new ArrayList<>();

        for (Node node : nodeList) {
            if (root != node) {
                if (node.getData(index) <= root.getData(index))
                    leftNodeList.add(node);
                else
                    rightNodeList.add(node);
            }
        }

        int newIndex = (index + 1) % root.data.length;
        root.left = build(leftNodeList, newIndex);
        root.right = build(rightNodeList, newIndex);

        if (root.left != null)
            root.left.parent = root;
        if (root.right != null)
            root.right.parent = root;
        return root;
    }

    public List<Node> searchKNN(Node root, Node q, int k) {
        List<Node> knnList = new ArrayList<>();
        searchBrother(knnList, root, q, k);
        return knnList;
    }

    public void searchBrother(List<Node> knnList, Node root, Node q, int k) {
        Node leafNNode = searchLeaf(root, q);
        double curD = q.computeDistance(leafNNode);//最近近似点与查询点的距离 也就是球体的半径
        leafNNode.distance = curD;
        maintainMaxHeap(knnList, leafNNode, k);
        while (leafNNode != root) {
            if (getBrother(leafNNode) != null) {
                Node brother = getBrother(leafNNode);
                if (curD > Math.abs(q.getData(leafNNode.parent.dim) - leafNNode.parent.getData(leafNNode.parent.dim)) || knnList.size() < k) {
                    //这样可能在另一个子区域中存在更加近似的点
                    searchBrother(knnList, brother, q, k);
                }
            }
            leafNNode = leafNNode.parent;//返回上一级
            leafNNode.distance = q.computeDistance(leafNNode);
            maintainMaxHeap(knnList, leafNNode, k);
        }
    }

    public Node getBrother(Node node) {
        if (node == node.parent.left) return node.parent.right;
        else return node.parent.left;
    }

    /**
     * 查询到叶子节点
     */
    public Node searchLeaf(Node root, Node q) {
        Node leaf = root, next;
        int index = 0;
        while (leaf.left != null || leaf.right != null) {
            if (q.getData(index) < leaf.getData(index)) {
                next = leaf.left;
            } else if (q.getData(index) > leaf.getData(index)) {
                next = leaf.right;
            } else {
                //当取到中位数时  判断左右子区域哪个更加近
                if (q.computeDistance(leaf.left) < q.computeDistance(leaf.right))
                    next = leaf.left;
                else
                    next = leaf.right;
            }
            if (next == null) break;//下一个节点是空时  结束了
            else {
                leaf = next;
                index = (index + 1) % root.data.length;
            }
        }
        return leaf;
    }

    //维护k个节点的最大堆
    public void maintainMaxHeap(List<Node> listNode, Node newNode, int k) {
        if (listNode.size() < k) {
            maxHeapFixUp(listNode, newNode);
        } else if (newNode.distance < listNode.get(0).distance) {
            maxHeapFixDown(listNode, newNode);
        }
    }

    private void maxHeapFixDown(List<Node> listNode, Node newNode) {
        listNode.set(0, newNode);
        int i = 0;
        int j = i * 2 + 1;
        while (j < listNode.size()) {
            if (j + 1 < listNode.size() && listNode.get(j).distance < listNode.get(j + 1).distance)
                j++;//选出子结点中较大的点，第一个条件是要满足右子树不为空
            if (listNode.get(i).distance >= listNode.get(j).distance) break;

            Node t = listNode.get(i);
            listNode.set(i, listNode.get(j));
            listNode.set(j, t);

            i = j;
            j = i * 2 + 1;
        }
    }

    private void maxHeapFixUp(List<Node> listNode, Node newNode) {
        listNode.add(newNode);
        int j = listNode.size() - 1;
        int i = (j + 1) / 2 - 1;
        while (i >= 0) {
            if (listNode.get(i).distance >= listNode.get(j).distance) break;
            Node t = listNode.get(i);
            listNode.set(i, listNode.get(j));
            listNode.set(j, t);

            j = i;
            i = (j + 1) / 2 - 1;
        }
    }

    private Node findMedian(List<Node> nodeList, int index) {
        int left = 0, right = nodeList.size() - 1, pos, mid = nodeList.size() / 2;
        do {
            pos = partition(nodeList, left, right, index);
            if (pos > mid) {
                right = pos - 1;
            } else {
                left = pos + 1;
            }
        } while (pos != mid);
        return nodeList.get(pos);
    }

    private int partition(List<Node> nodeList, int left, int right, int index) {
        Node temp = nodeList.get(left);
        while (left < right) {
            while (left < right && nodeList.get(right).data[index] >= temp.data[index]) {
                right--;
            }
            nodeList.set(left, nodeList.get(right));
            while (left < right && nodeList.get(left).data[index] <= temp.data[index]) {
                left++;
            }
            nodeList.set(right, nodeList.get(left));
        }
        nodeList.set(left, temp);
        return left;
    }

    public static class Node {
        public double[] data;//树上节点的数据，是一个多维向量
        public double distance;//与当前查询点的距离，初始化的时候是没有的
        public Node left, right, parent;
        public int dim = -1;//这个节点按第几个维度分割
        public int movieId;

        public Node(int movieId,double[] data) {
            this.movieId=movieId;
            this.data = data;
        }

        public double getData(int index) {
            if (data == null || data.length <= index)
                return Integer.MIN_VALUE;
            return data[index];
        }

        //距离可有其他算法，欧式、余弦、曼哈顿...
        public double computeDistance(Node that) {
            if (this.data == null || that == null || that.data == null || this.data.length != that.data.length)
                return Double.MAX_VALUE;
            double d = 0;
            for (int i = 0; i < this.data.length; i++) {
                d += Math.pow(this.data[i] - that.data[i], 2);
            }

            return Math.sqrt(d);
        }
    }
}