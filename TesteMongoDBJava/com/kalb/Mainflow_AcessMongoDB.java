package com.kalb;

import java.util.Date;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.MbElement;
import com.ibm.broker.plugin.MbException;
import com.ibm.broker.plugin.MbJSON;
import com.ibm.broker.plugin.MbMessage;
import com.ibm.broker.plugin.MbMessageAssembly;
import com.ibm.broker.plugin.MbOutputTerminal;
import com.ibm.broker.plugin.MbUserException;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;

public class Mainflow_AcessMongoDB extends MbJavaComputeNode {
	public static MongoClient mongoClient = null;

	public void evaluate(MbMessageAssembly inAssembly) throws MbException {
		MbOutputTerminal out = getOutputTerminal("out");
		String udp_DataBase_Name=(String)getUserDefinedAttribute("DataBase_Name");
		MbMessage inMessage = inAssembly.getMessage();
		MbMessageAssembly outAssembly = null;
		try {
			// ----------------------------------------------------------
			// Add user code below
			MongoClient mongo = new MongoClient("localhost", 27017);
			MongoDatabase db = mongo.getDatabase(udp_DataBase_Name);
			MongoCollection<Document> collection = db.getCollection("Cliente");
			int count = 0;
			MbElement clientes = inMessage.getRootElement().getFirstElementByPath("JSON/Data/Clientes/Item");
			while (clientes != null) {
				Document document = new Document();
				document.put("nome", clientes.getFirstElementByPath("nome").getValue());
				document.put("idade", clientes.getFirstElementByPath("idade").getValue());
				document.put("dataHoraCriacao", new Date());
				if (!(existeRegistroNaBase(db, clientes.getFirstElementByPath("nome").getValue().toString(), (int)clientes.getFirstElementByPath("idade").getValue()))){ //to avoid duplicates in database
					collection.insertOne(document);
					count++;
				}				
				clientes = clientes.getNextSibling();
			}
			// Creating OK response
			MbMessage outMessage = new MbMessage();
			copyMessageHeaders(inMessage, outMessage);
			outAssembly = new MbMessageAssembly(inAssembly, outMessage);
			MbElement outRoot = outMessage.getRootElement();
			MbElement outJsonRoot = outRoot
					.createElementAsLastChild(MbJSON.PARSER_NAME); // Create JSON message
			MbElement data = outJsonRoot.createElementAsLastChild(
					MbElement.TYPE_NAME, MbJSON.DATA_ELEMENT_NAME, null); // Create Data element, must be first after JsonParser
			MbElement reponse = data.createElementAsLastChild(MbJSON.OBJECT, "response", null); // Create a custom response object
			reponse.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "code",	0); // code 0 = OK message
			reponse.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", "Insert " + count + " rows");
			// End of user code
			// ----------------------------------------------------------
		} catch (MbException e) {
			// Re-throw to allow Broker handling of MbException
			throw e;
		} catch (RuntimeException e) {
			// Re-throw to allow Broker handling of RuntimeException
			throw e;
		} catch (Exception e) {
			// Consider replacing Exception with type(s) thrown by user code
			// Example handling ensures all exceptions are re-thrown to be
			// handled in the flow
			throw new MbUserException(this, "evaluate()", "", "", e.toString(),
					null);
		}
		// The following should only be changed
		// if not propagating message to the 'out' terminal
		out.propagate(outAssembly);
	}

	public boolean existeRegistroNaBase(MongoDatabase db, String nome, int idade) {
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put("nome", nome);
		searchQuery.put("idade", idade);
		MongoCollection<Document> table = db.getCollection("Cliente");
		FindIterable<Document> cursor = table.find(searchQuery);
		boolean existeRegistroNaBase = false;
		for (Document current : cursor) {
			existeRegistroNaBase = true;
			break;
		}
		return existeRegistroNaBase;
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
