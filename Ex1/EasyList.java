package HadoopProject;

public class EasyList 
{
	public static class Node{
		public int value;
		public Node next = null;
		Node(int value){
			this.value = value;
		}
		Node(int value, Node next){
			this.value = value;
			this.next = next;
		}
		public void setValue(int value){
			this.value = value;
		}
	}
	private static void printLinkList(Node head) {
Node cnode=head;
		
		while(cnode.next!=null){
			System.out.print(cnode.value);
			cnode = cnode.next;
			System.out.print("->");
		}
		System.out.print(cnode.value);
	}
	

  public static void main( String[] args ){
    int[] arr = {1,3,5,7,2,4};
    Node head = createLinkList(arr);
    printLinkList(head);
  }
	private static Node createLinkList(int[] arr) {
		// TODO Auto-generated method stub
		Node[] linkArr = new Node[arr.length];
		for (int i=0; i<arr.length; i++){
			linkArr[i] = new Node(arr[i]);
		}
		for (int i=0; i<arr.length; i++){
			linkArr[i].setValue(arr[i]);
			if (i == arr.length - 1){
				linkArr[i].next = null;
			}
			else{
				linkArr[i].next = linkArr[i+1];
			}
		}
		return linkArr[0];
	}
}
