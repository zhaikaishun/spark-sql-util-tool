package me.kaishun.spark_main;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import me.kaishun.spark_v20.DataFilterSpark;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * 读取XML文件信息
 * 
 * @author Zhaikaishun
 *
 */
@SuppressWarnings("unchecked")
public class FilterReadFromXml {
	private String configPath = "conf/people.xml";

	public ArrayList<String> getTableName1() throws DocumentException {
		ArrayList<String> tableNameArray = new ArrayList<String>();

		SAXReader reader = new SAXReader();

		Document document = reader.read(new File(configPath));

		String xpathtable = "//tables//table";
		List<String> list = document.selectNodes(xpathtable);
		Iterator iter = list.iterator();
		while (iter.hasNext()) {
			Element element = (Element) iter.next();
			tableNameArray.add(element.attribute(0).getValue());

		}
		return tableNameArray;
	}

	/**
	 *
	 * @return 返回conf_table下的文件名，去掉xml，此文件名将会作为数据库表名
	 */
	public ArrayList<String> getTableName() {
		String path = "conf_table"; // 路径
		File f = new File(path);
		if (!f.exists()) {
			System.out.println(path + " not exists");
			return null;
		}
		File fa[] = f.listFiles();
		ArrayList<String> arrayList = new ArrayList<String>();
		for (int i = 0; i < fa.length; i++) {
			File fs = fa[i];
			if (fs.isDirectory()) {
				System.out.println(fs.getName() + " [目录]");
			} else {
				System.out.println(fs.getName().replace(".xml", ""));
				arrayList.add(fs.getName().replace(".xml", ""));
			}
		}
		return arrayList;
	}

	/**
	 * @paramW
	 * @return 返回一个 模板表FormModel
	 * @see FormModel
	 * @throws DocumentException
	 */
	public FormModel getFormModelFromTableName(String tablename) throws DocumentException {

		FormModel formModel = new FormModel();

		SAXReader reader = new SAXReader();
		configPath = "conf_table/" + tablename + ".xml";
		Document document = reader.read(new File(configPath));

		// String xpath =
		// "//tables//table[@tablename='"+tablename+"']//fieldname";
		String xpath = "//table//fieldname";
		List<String> list = document.selectNodes(xpath);
		Iterator iter = list.iterator();
		while (iter.hasNext()) {
			Element element = (Element) iter.next();
			// connect the field
			FieldAttr fieldAttr = new FieldAttr(element.attribute(0).getValue(),
					element.attribute(1).getValue(),element.attribute(2).getValue());
			formModel.fieldAttrArrayList.add(fieldAttr);
		}

		//得到分隔符
		String xpathSplit = "//table//splitsign";
		List<Element> signlist = document.selectNodes(xpathSplit);
		Element splitElement = signlist.get(0);
		String splitsign = (String)splitElement.getData();
		System.out.println("splitSin: "+splitsign);
		formModel.setSplitSign(splitsign);

		//得到输入路径
		String xpathInputPath = "//table//inputPath";
		List<Element> inputLists = document.selectNodes(xpathInputPath);
		Element inputPathElement = inputLists.get(0);
		String inputPath = (String)inputPathElement.getData();
		formModel.setInputPath(inputPath);

		//设置表名
		formModel.setTableName(tablename);
		return formModel;
	}

	public ArrayList deleteFile(String path) {
		// String path = "conf_table"; // 路径
		File f = new File(path);
		if (!f.exists()) {
			System.out.println(path + " not exists");
			return null;
		}
		File fa[] = f.listFiles();
		ArrayList<String> arrayList = new ArrayList<String>();
		for (int i = 0; i < fa.length; i++) {
			File fs = fa[i];
			if (fs.isDirectory()) {
				// System.out.println(fs.getName() + " [目录]");
			} else {
				System.out.println(fs.getName().replace(".xml", ""));
				arrayList.add(fs.getName().replace(".xml", ""));
				if (!fs.getName().startsWith("part-")) {
					fs.delete();
				}
			}
		}
		return arrayList;
	}

}
