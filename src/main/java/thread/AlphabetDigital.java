package main.java.thread;

/**
 * 请用两个线程交替输出A1B2C3D4...，A线程输出字母，B线程输出数字，要求A线程首先执行，B线程其次执行！
 * @author caik
 * @since 2020年12月16日
 */
public class AlphabetDigital {

	public static void main(String[] args) {
		final CharClass c = new CharClass();

		new Thread(new Runnable() {

			@Override
			public void run() {
				while (c.hasNext()) {
					synchronized (c) {
						System.out.print(c.getAlphabet());
						c.notifyAll();
						try {
							if(c.hasNext()) {
								c.wait();
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}

			}
		}).start();

		new Thread(new Runnable() {

			@Override
			public void run() {
				while (c.hasNext()) {
					synchronized (c) {
						System.out.print(c.getDigital());
						c.notifyAll();
						try {
							if(c.hasNext()) {
								c.wait();
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}

			}
		}).start();

	}

}

class CharClass {
	int alphabet = 65;

	int digital = 1;

	public char getAlphabet() {
		return (char) alphabet++;
	}

	public int getDigital() {
		return digital++;
	}

	public boolean hasNext() {
		return alphabet <= 'Z';
	}

}
