package com.kalb;

import org.bson.Document;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.MbElement;
import com.ibm.broker.plugin.MbException;
import com.ibm.broker.plugin.MbJSON;
import com.ibm.broker.plugin.MbMessage;
import com.ibm.broker.plugin.MbMessageAssembly;
import com.ibm.broker.plugin.MbOutputTerminal;
import com.ibm.broker.plugin.MbUserException;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class GetAllClientes extends MbJavaComputeNode {

	public void evaluate(MbMessageAssembly inAssembly) throws MbException {
		MbOutputTerminal out = getOutputTerminal("out");
		String udp_DataBase_Name=(String)getUserDefinedAttribute("DataBase_Name");
		MbMessage inMessage = inAssembly.getMessage();
		MbMessageAssembly outAssembly = null;
		try {
			//mongo db connection
			MongoClient mongo = new MongoClient("localhost", 27017);
			MongoDatabase db = mongo.getDatabase(udp_DataBase_Name);
			MongoCollection<Document> collection = db.getCollection("Cliente");
			//create response message
			MbMessage outMessage = new MbMessage();
			copyMessageHeaders(inMessage, outMessage);
			outAssembly = new MbMessageAssembly(inAssembly, outMessage);
			MbElement outRoot = outMessage.getRootElement();
			MbElement outJsonRoot = outRoot.createElementAsLastChild(MbJSON.PARSER_NAME); // Create JSON message
			MbElement data = outJsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, MbJSON.DATA_ELEMENT_NAME, null); // Create Data element, must be first after JsonParser
			MbElement reponse = data.createElementAsLastChild(MbJSON.OBJECT, "response", null); // Create a custom response object
			MbElement cliente = reponse.createElementAsLastChild(MbJSON.ARRAY, "cliente", null); // Create clientes array
			//find all clientes
			FindIterable<Document> cursor = collection.find();
			for (Document current : cursor) {
				//add to output message
				MbElement item = cliente.createElementAsLastChild(MbElement.TYPE_NAME,MbJSON.ARRAY_ITEM_NAME,null);
				item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "nome", current.getString("nome"));
				item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "idade", current.getInteger("idade"));
			}
		} catch (RuntimeException e) {
			// Re-throw to allow Broker handling of RuntimeException
			throw e;
		} catch (Exception e) {
			// Consider replacing Exception with type(s) thrown by user code
			// Example handling ensures all exceptions are re-thrown to be handled in the flow
			throw new MbUserException(this, "evaluate()", "", "", e.toString(),
					null);
		}
		// The following should only be changed
		// if not propagating message to the 'out' terminal
		out.propagate(outAssembly);
	}
	
	public void copyMessageHeaders(MbMessage inMessage, MbMessage outMessage)
			throws MbException {
		MbElement outRoot = outMessage.getRootElement();
		MbElement header = inMessage.getRootElement().getFirstChild();

		while (header != null && header.getNextSibling() != null) {
			outRoot.addAsLastChild(header.copy());
			header = header.getNextSibling();
		}
	}
}
