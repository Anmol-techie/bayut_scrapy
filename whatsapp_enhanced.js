const puppeteer = require('puppeteer');
const path = require('path');

(async () => {
  const phoneNumbers = ['9589175895','9404525147','7212132143'];
  const messageTemplate = 'Hello, this is a test message!';
  const photoPath = '/Users/apple/Desktop/mcp/photo.png';

  const browser = await puppeteer.launch({ headless: false });
  const page = await browser.newPage();

  try {
    await page.goto('https://web.whatsapp.com', { waitUntil: 'networkidle2' });

    console.log('Waiting for 30 seconds to allow UI changes...');
    await new Promise(resolve => setTimeout(resolve, 30000));

    const searchBarSelector = 'div[aria-label="Search input textbox"][contenteditable="true"]';
    console.log('Checking for search bar...');
    const searchBar = await page.waitForSelector(searchBarSelector, { timeout: 10000 });
    if (!searchBar) {
      throw new Error('Search bar not found. Ensure you are logged in to WhatsApp Web.');
    }
    console.log('Search bar found.');

    for (const phoneNumber of phoneNumbers) {
      console.log(`\n📱 Processing phone number: ${phoneNumber}`);
      
      await page.click(searchBarSelector);
      await page.keyboard.down('Control');
      await page.keyboard.press('KeyA');
      await page.keyboard.up('Control');
      await page.keyboard.press('Backspace');
      
      console.log(`Typing phone number: ${phoneNumber}`);
      await page.type(searchBarSelector, phoneNumber);
      await page.keyboard.press('Enter');
      console.log('Search triggered.');

      await new Promise(resolve => setTimeout(resolve, 2000));

      const noResultsSelector = 'span._ao3e:has-text("No chats, contacts or messages found")';
      const noResults = await page.$(noResultsSelector);
      
      if (noResults) {
        console.log(`❌ Number ${phoneNumber} not found. Trying to add as new contact...`);
        
        const newChatButtonSelector = 'span[data-icon="new-chat-outline"]';
        const newChatButton = await page.$(newChatButtonSelector);
        
        if (newChatButton) {
          await page.click(newChatButtonSelector);
          console.log('New chat button clicked.');
          await new Promise(resolve => setTimeout(resolve, 1500));
          
          const phoneInputSelector = 'p.selectable-text.copyable-text.x15bjb6t.x1n2onr6';
          const phoneInput = await page.waitForSelector(phoneInputSelector, { timeout: 5000 });
          
          if (phoneInput) {
            await page.click(phoneInputSelector);
            const fullNumber = phoneNumber.startsWith('+') ? phoneNumber : `+91${phoneNumber}`;
            await page.type(phoneInputSelector, fullNumber);
            console.log(`Entered number: ${fullNumber}`);
            
            await page.keyboard.press('Enter');
            await new Promise(resolve => setTimeout(resolve, 3000));
            
            const notInContactsSelector = 'span:has-text("Not in your contacts")';
            const notInContacts = await page.$(notInContactsSelector);
            
            if (notInContacts) {
              console.log(`✅ Number ${phoneNumber} exists on WhatsApp but is not saved.`);
              
              const contactNumberSelector = 'span[title*="+"]._ao3e, span[title*="91"]._ao3e';
              const contactNumber = await page.$(contactNumberSelector);
              
              if (contactNumber) {
                await contactNumber.click();
                console.log('Clicked on the unsaved contact.');
                await new Promise(resolve => setTimeout(resolve, 2000));
              }
            } else {
              const errorMessageSelector = 'div:has-text("Phone number shared via url is invalid")';
              const invalidNumber = await page.$(errorMessageSelector);
              
              if (invalidNumber) {
                console.log(`⚠️ Number ${phoneNumber} is not on WhatsApp. Skipping...`);
                continue;
              }
            }
          }
        }
      } else {
        const searchResultsSelector = 'div[aria-label="Search results."]';
        console.log('Waiting for search results...');
        
        try {
          const searchResults = await page.waitForSelector(searchResultsSelector, { timeout: 5000 });
          
          if (searchResults) {
            console.log('Search results loaded.');
            
            const firstContactSelector = 'div[aria-label="Search results."] div[role="listitem"]:first-child';
            const firstContact = await page.waitForSelector(firstContactSelector, { timeout: 5000 });
            
            if (firstContact) {
              await page.click(firstContactSelector);
              console.log(`✅ Clicked on saved contact: ${phoneNumber}`);
              await new Promise(resolve => setTimeout(resolve, 2000));
            }
          }
        } catch (e) {
          console.log(`No search results for ${phoneNumber}, checking if it's a direct number...`);
          continue;
        }
      }

      const messageInputSelector = 'div[aria-label="Type a message"] p.selectable-text.copyable-text';
      const chatLoaded = await page.$(messageInputSelector);
      
      if (!chatLoaded) {
        console.log(`❌ Could not open chat for ${phoneNumber}. Skipping...`);
        continue;
      }

      try {
        console.log('Looking for plus button (attachment)...');
        
        const plusButtonSelector = 'span[data-icon="plus-rounded"]';
        const plusButton = await page.waitForSelector(plusButtonSelector, { timeout: 5000 });
        
        if (!plusButton) {
          throw new Error('Plus button not found');
        }
        
        await page.click(plusButtonSelector);
        console.log('Plus button clicked, attachment menu should open.');
        
        await new Promise(resolve => setTimeout(resolve, 1500));

        console.log('Looking for hidden file input for photos...');
        
        const hiddenFileInputSelector = 'li:has(span[data-icon="media-filled-refreshed"]) input[type="file"]';
        
        let fileInput;
        try {
          fileInput = await page.waitForSelector(hiddenFileInputSelector, { timeout: 3000 });
        } catch (e) {
          const allFileInputs = await page.$$('input[type="file"][accept*="image"]');
          if (allFileInputs.length > 0) {
            fileInput = allFileInputs[0];
          } else {
            throw new Error('No suitable file input found');
          }
        }

        if (!fileInput) {
          throw new Error('File input not found');
        }

        const absolutePhotoPath = path.resolve(photoPath);
        console.log(`Uploading photo from: ${absolutePhotoPath}`);
        await fileInput.uploadFile(absolutePhotoPath);
        console.log('Photo uploaded successfully.');

        await new Promise(resolve => setTimeout(resolve, 3000));

        console.log('Adding caption to photo...');
        
        const captionSelector = 'div[aria-label="Add a caption"][contenteditable="true"]';
        
        try {
          const captionInput = await page.waitForSelector(captionSelector, { timeout: 5000 });
          if (captionInput) {
            await page.click(captionSelector);
            await page.type(captionSelector, messageTemplate);
            console.log('Caption added to photo.');
          }
        } catch (e) {
          console.log('Caption field not found, sending photo without caption...');
        }

        console.log('Looking for send button...');
        
        const photoSendButtonSelector = 'span[data-icon="wds-ic-send-filled"]';
        const photoSendButton = await page.waitForSelector(photoSendButtonSelector, { timeout: 10000 });
        
        if (!photoSendButton) {
          throw new Error('Photo send button not found');
        }
        
        await page.click(photoSendButtonSelector);
        console.log(`✅ Photo with message sent successfully to: ${phoneNumber}`);

      } catch (photoError) {
        console.error(`Failed to send photo to ${phoneNumber}:`, photoError);
        
        try {
          console.log(`Attempting to send text message only to: ${phoneNumber}`);
          
          const messageInput = await page.waitForSelector(messageInputSelector, { timeout: 5000 });
          if (messageInput) {
            await page.click(messageInputSelector);
            await page.type(messageInputSelector, messageTemplate);
            
            const sendButtonSelector = 'button[aria-label="Send"]';
            const sendButton = await page.waitForSelector(sendButtonSelector, { timeout: 5000 });
            if (sendButton) {
              await page.click(sendButtonSelector);
              console.log(`✅ Text message sent as fallback to: ${phoneNumber}`);
            }
          }
        } catch (fallbackError) {
          console.error(`Failed to send even text message to ${phoneNumber}:`, fallbackError);
        }
      }

      await new Promise(resolve => setTimeout(resolve, 4000));
    }
    
    console.log('\n🎉 All messages processed!');
    
  } catch (error) {
    console.error('Error during automation:', error);
  } finally {
    await browser.close();
  }
})();