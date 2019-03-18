#--
# Created as a contribution for the PySphere project 
# by Ezequiel Ruiz (https://github.com/emruiz81)
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#   * Neither the name of copyright holders nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
#--

from pysphere import VIProperty, VIMor
from pysphere.resources import VimService_services as VI
from pysphere import VIException, VIApiException, FaultTypes


class VIEventHistoryCollector(object):
    """
    EventHistoryCollector provides a mechanism for retrieving historical data and 
    updates when the server appends new events.
    """

    def __init__(self, server, include_types=None, include_users=None, beginTime=None, endTime=None):
        """Creates an Event History Collector that gathers Event objects based on the 
        provides filters.
          * server: the connected VIServer instance
          * include_types: if provided, limits the set of collected events by their 
            types. Should be a list of possible types (like 'UserLoginSessionEvent')
          * include_users [tuple([user1,userx]:list,system:boolean]: if provided, limits the set of 
            collected events to those events produced by any of the users indicated in the list.
            The format is a tuple with a list of possible users as the first parameter, and a 
            boolean value to indicate if system user is included or not as second parameter.
          * beginTime [time tuple]: The time from which available history 
            events are gathered. Corresponds to server time. When the beginTime
            is omitted, the returned events start from the first available
            event in the system.
          * endTime [time tuple]: The time up to which available history
            events are gathered. Corresponds to server time. When the endTime
            is omitted, the returned result includes up to the most recent
            event.
        """

        self._server = server
        self._mor = None
        
        try:
            event_manager = server._do_service_content.EventManager
            request = VI.CreateCollectorForEventsRequestMsg()
            _this = request.new__this(event_manager)
            _this.set_attribute_type(event_manager.get_attribute_type())
            request.set_element__this(_this)
                   
            _filter = request.new_filter()
            
            if(beginTime or endTime):
                do_EventFilterSpecByTime = _filter.new_time()
                
                if(beginTime):
                    do_EventFilterSpecByTime.set_element_beginTime(beginTime)

                if(endTime):
                    do_EventFilterSpecByTime.set_element_endTime(endTime)

                _filter.set_element_time(do_EventFilterSpecByTime)
            
            if(include_types):
                _filter.set_element_eventTypeId(include_types)

            if(include_users):
                do_EventFilterSpecByUser = _filter.new_userName()
                
                # Mandatory parameter
                do_EventFilterSpecByUser.set_element_systemUser(include_users[1])
                
                # Only add the list of users is there is at least one entry
                if(include_users[0]):
                    do_EventFilterSpecByUser.set_element_userList(include_users[0])

                _filter.set_element_userName(do_EventFilterSpecByUser)
                
            request.set_element_filter(_filter)
            resp = server._proxy.CreateCollectorForEvents(request)._returnval
        
        except (VI.ZSI.FaultException), e:
            raise VIApiException(e)
        
        self._mor = resp
        self._props = VIProperty(self._server, self._mor)
        
    def reset_collector(self):
        """
        Moves the "scrollable view" to the item immediately preceding the "viewable latest page". 
        If you use ReadPreviousEvents, all items are retrieved from the newest item to the oldest item.
        """
        
        request = VI.ResetCollectorRequestMsg()
        _this = request.new__this(self._mor)
        _this.set_attribute_type(self._mor.get_attribute_type())
        request.set_element__this(_this)
        
        self._server._proxy.ResetCollector(request)
        
    def rewind_collector(self):
        """
        Moves the "scrollable view" to the oldest item. 
        If you use ReadNextEvents, all items are retrieved from the oldest item to the newest item. 
        This is the default setting when the collector is created.
        """
        
        request = VI.RewindCollectorRequestMsg()
        _this = request.new__this(self._mor)
        _this.set_attribute_type(self._mor.get_attribute_type())
        request.set_element__this(_this)
        
        self._server._proxy.RewindCollector(request)
        
    def destroy_collector(self):
        """
        Destroys this collector.
        """
        
        request = VI.DestroyCollectorRequestMsg()
        _this = request.new__this(self._mor)
        _this.set_attribute_type(self._mor.get_attribute_type())
        request.set_element__this(_this)
        
        self._server._proxy.DestroyCollector(request)
        
    def set_collector_page_size(self,maxCount):
        """
        Sets the "viewable latest page" size to contain at most the number of items specified by the maxCount parameter).
        """
        
        request = VI.SetCollectorPageSizeRequestMsg()
        _this = request.new__this(self._mor)
        _this.set_attribute_type(self._mor.get_attribute_type())
        request.set_element__this(_this)
        request.set_element_maxCount(maxCount)
        
        self._server._proxy.SetCollectorPageSize(request)
        
    def get_latest_events(self):
        """
        Reads the items in the 'viewable latest page'. 
        As new events that match the collector's EventFilterSpec are created, they are added to this page, 
        and the oldest events are removed from the collector to keep the size of the page to that allowed by 
        HistoryCollector#setLatestPageSize.
        The "oldest event" is the one with the smallest key (event ID). The events in the returned page are unordered. 
        """
        
        self._props._flush_cache()
        if not hasattr(self._props, "latestPage"):
            return []
        
        ret = []
        for event in self._props.latestPage:
            ret.append(event)
        return ret
    
    def read_next_events(self, max_count):
        """
        Reads the 'scrollable view' from the current position. 
        The scrollable position is moved to the next newer page after the read. No item is returned when the end of the collector is reached.
        """
        
        return self.__read_events(max_count, True)
    
    def read_previous_events(self, max_count):
        """
        Reads the 'scrollable view' from the current position. 
        The scrollable position is moved to the next older page after the read. No item is returned when the head of the collector is reached.
        """

        return self.__read_events(max_count, False)
    
    def __read_events(self, max_count, next_page):
        """
        Reads the 'scrollable view' from the current position. 
        """
        
        if not isinstance(max_count, int):
            raise VIException("max_count should be an integer", 
                              FaultTypes.PARAMETER_ERROR)
        
        if next_page:
            request = VI.ReadNextEventsRequestMsg()
        else:
            request = VI.ReadPreviousEventsRequestMsg()
            
        _this = request.new__this(self._mor)
        _this.set_attribute_type(self._mor.get_attribute_type())
        request.set_element__this(_this)
        
        request.set_element_maxCount(max_count)
        try:
            if next_page:
                resp = self._server._proxy.ReadNextEvents(request)._returnval
            else:
                resp = self._server._proxy.ReadPreviousEvents(request)._returnval
            
            ret = []
            for event in resp:
                ret.append(VIProperty(self._server,event))
        
        except (VI.ZSI.FaultException), e:
            raise VIApiException(e)
        
        return ret